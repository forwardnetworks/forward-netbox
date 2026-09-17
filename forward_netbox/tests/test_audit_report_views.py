# The nine operator audits as pages on the sync (2.9.6): the index, the two
# report shapes (live from NetBox, latest job from Forward), the button that
# runs a Forward-backed audit, and the rule that a GET never calls Forward.
from io import StringIO
from unittest.mock import Mock
from unittest.mock import patch

from core.choices import JobStatusChoices
from core.models import Job
from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Interface
from dcim.models import Manufacturer
from dcim.models import Site
from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.contrib.messages import get_messages
from django.core.management import call_command
from django.test import Client
from django.test import TestCase
from django.urls import reverse
from ipam.models import VLAN

from forward_netbox.jobs import AuditPrimaryIpJob
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.audit_reports import AUDIT_REPORTS
from forward_netbox.utilities.audit_reports import FORWARD_BACKED_AUDITS
from forward_netbox.utilities.audit_reports import run_audit_job
from forward_netbox.utilities.routing_dangling_audit import (
    audit_routing_dangling_rows,
)
from forward_netbox.utilities.sync_facade import BUTTON_JOB_SPECS


class AuditReportViewTest(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="audit-src",
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
            name="audit-sync",
            source=self.source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        mfr = Manufacturer.objects.create(name="MfrA", slug="mfr-a")
        self.dt = DeviceType.objects.create(manufacturer=mfr, model="dt-a", slug="dt-a")
        self.role = DeviceRole.objects.create(name="RoleA", slug="role-a")
        self.site = Site.objects.create(name="SiteA", slug="site-a")
        self.other_site = Site.objects.create(name="SiteB", slug="site-b")

    def _client(self, *, superuser=True, actions=()):
        user = get_user_model().objects.create_user(
            username=f"audit-{superuser}-{'-'.join(actions)}", password="x"
        )
        if superuser:
            user.is_superuser = True
            user.is_staff = True
            user.save()
        elif actions:
            from core.models import ObjectType
            from users.models import ObjectPermission

            permission = ObjectPermission.objects.create(
                name=f"audit test {'-'.join(actions)}", actions=list(actions)
            )
            permission.object_types.add(ObjectType.objects.get_for_model(ForwardSync))
            permission.users.add(user)
        client = Client()
        client.force_login(user)
        return client

    def _index_url(self):
        return reverse(
            "plugins:forward_netbox:forwardsync_audits", kwargs={"pk": self.sync.pk}
        )

    def _report_url(self, key):
        return reverse(
            "plugins:forward_netbox:forwardsync_audit_report",
            kwargs={"pk": self.sync.pk, "audit": key},
        )

    def _job(self, report, data, status=JobStatusChoices.STATUS_COMPLETED):
        return Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name=f"{self.sync.name} - {report.job_suffix}",
            status=status,
            data=data,
            job_id="123e4567-e89b-12d3-a456-426614174777",
        )

    # --- registry ----------------------------------------------------------

    def test_every_forward_backed_audit_is_a_button_kind(self):
        for report in FORWARD_BACKED_AUDITS:
            with self.subTest(audit=report.key):
                spec = BUTTON_JOB_SPECS[report.kind]
                self.assertEqual(spec[1], report.job_suffix)
        self.assertEqual(
            {report.kind for report in FORWARD_BACKED_AUDITS},
            {kind for kind in BUTTON_JOB_SPECS if kind.startswith("audit_")},
        )

    def test_every_presenter_survives_an_empty_payload(self):
        for report in AUDIT_REPORTS.values():
            with self.subTest(audit=report.key):
                sections = report.present({})
                self.assertIn("summary", sections)
                self.assertIn("tables", sections)

    # --- index -------------------------------------------------------------

    def test_index_lists_every_audit_and_the_latest_job(self):
        report = AUDIT_REPORTS["primary_ip"]
        job = self._job(report, {"audit": "primary_ip", "payload": {}})
        response = self._client().get(self._index_url())
        self.assertEqual(response.status_code, 200)
        for entry in AUDIT_REPORTS.values():
            self.assertContains(response, str(entry.title))
            self.assertContains(response, self._report_url(entry.key))
        self.assertContains(response, f"#{job.pk}")

    def test_index_requires_view_permission(self):
        client = self._client(superuser=False)
        self.assertEqual(client.get(self._index_url()).status_code, 403)

    def test_unknown_audit_is_404(self):
        self.assertEqual(self._client().get(self._report_url("nope")).status_code, 404)

    # --- live (database-only) reports --------------------------------------

    def test_interface_vlan_audit_renders_live_with_links(self):
        device = Device.objects.create(
            name="dev-vlan", device_type=self.dt, role=self.role, site=self.site
        )
        vlan = VLAN.objects.create(name="v-other", vid=200, site=self.other_site)
        interface = Interface.objects.create(
            device=device,
            name="eth0",
            type="1000base-t",
            mode="access",
            untagged_vlan=vlan,
        )
        with patch.object(ForwardSource, "get_client") as get_client:
            response = self._client().get(self._report_url("interface_untagged_vlans"))
        get_client.assert_not_called()
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Computed from NetBox")
        self.assertContains(
            response, reverse("dcim:interface", kwargs={"pk": interface.pk})
        )
        self.assertContains(response, reverse("dcim:device", kwargs={"pk": device.pk}))
        self.assertContains(response, "v-other (200)")

    def test_device_name_ambiguity_renders_live(self):
        for site in (self.site, self.other_site):
            Device.objects.create(
                name="twin", device_type=self.dt, role=self.role, site=site
            )
        response = self._client().get(self._report_url("device_name_ambiguity"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "twin")
        self.assertContains(response, "SiteB")

    def test_routing_dangling_renders_live_and_the_command_agrees(self):
        response = self._client().get(self._report_url("routing_dangling"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Dangling BGP routers")
        report = audit_routing_dangling_rows()
        self.assertEqual(sum(report["dangling"].values()), 0)
        out = StringIO()
        call_command("forward_routing_dangling_audit", stdout=out)
        self.assertIn('"dangling"', out.getvalue())

    def test_a_live_report_post_only_redirects(self):
        response = self._client().post(self._report_url("routing_dangling"))
        self.assertEqual(response.status_code, 302)
        # Not Job.objects.count(): NetBox's own search-cache-update jobs fire
        # for every searchable object setUp creates, on this NetBox series -
        # unrelated to whether this POST enqueued an AUDIT job for the sync.
        self.assertEqual(self.sync.jobs.count(), 0)

    # --- Forward-backed reports --------------------------------------------

    def test_a_forward_backed_report_never_calls_forward_on_get(self):
        with patch.object(ForwardSource, "get_client") as get_client:
            response = self._client().get(self._report_url("primary_ip"))
        get_client.assert_not_called()
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "has not been run")
        self.assertContains(response, "Run audit")

    def test_a_forward_backed_report_renders_the_latest_job(self):
        report = AUDIT_REPORTS["primary_ip"]
        job = self._job(
            report,
            {
                "audit": "primary_ip",
                "payload": {
                    "snapshot_id": "snap-9",
                    "mgmt_tagged_devices": 3,
                    "resolvable": 1,
                    "unresolved": 2,
                    "unresolved_device_not_in_netbox": 1,
                    "unresolved_interface_not_matched": 1,
                    "unresolved_interface_present_no_ip": 0,
                    "example_device_not_in_netbox": ["ghost-1"],
                    "example_interface_not_matched": [["dev-2", ["Mgmt_Vl99"]]],
                    "example_interface_present_no_ip": [],
                },
            },
        )
        response = self._client().get(self._report_url("primary_ip"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, f"#{job.pk}")
        self.assertContains(response, "snap-9")
        self.assertContains(response, "ghost-1")
        self.assertContains(response, "Mgmt_Vl99")

    def test_a_failed_audit_job_shows_its_recorded_error(self):
        report = AUDIT_REPORTS["global_ipam"]
        self._job(
            report,
            {"audit": "global_ipam", "error": "Forward audit failed: HTTP 503"},
            status=JobStatusChoices.STATUS_ERRORED,
        )
        response = self._client().get(self._report_url("global_ipam"))
        self.assertContains(response, "HTTP 503")

    def test_run_button_enqueues_the_audit_kind(self):
        # A completed job with the same name does not count as active.
        real_job = self._job(AUDIT_REPORTS["primary_ip"], {})
        with patch(
            "forward_netbox.jobs.enqueue_forward_job", return_value=real_job
        ) as enqueue:
            response = self._client().post(self._report_url("primary_ip"))
        self.assertEqual(response.status_code, 302)
        enqueue.assert_called_once()
        self.assertEqual(
            enqueue.call_args.kwargs["name"],
            f"{self.sync.name} - audit primary IP resolution",
        )
        rendered = " ".join(str(m) for m in get_messages(response.wsgi_request))
        self.assertIn("Queued job", rendered)

    def test_run_button_requires_the_run_permission(self):
        client = self._client(superuser=False, actions=("view",))
        self.assertEqual(client.get(self._report_url("primary_ip")).status_code, 200)
        self.assertEqual(client.post(self._report_url("primary_ip")).status_code, 403)

    def test_run_audit_job_stores_the_payload_and_the_failure(self):
        report = AUDIT_REPORTS["primary_ip"]
        job = self._job(report, {}, status=JobStatusChoices.STATUS_RUNNING)
        with (
            patch.object(ForwardSource, "get_client", return_value=Mock()),
            patch.object(report, "run", return_value={"mgmt_tagged_devices": 4}),
        ):
            run_audit_job(job, report)
        job.refresh_from_db()
        self.assertEqual(job.data["payload"], {"mgmt_tagged_devices": 4})

        with (
            patch.object(ForwardSource, "get_client", return_value=Mock()),
            patch.object(report, "run", side_effect=RuntimeError("secret-detail")),
            self.assertRaises(RuntimeError),
        ):
            run_audit_job(job, report)
        job.refresh_from_db()
        self.assertEqual(job.data["audit"], "primary_ip")
        self.assertIn("error", job.data)
        self.assertNotIn("secret-detail", job.data["error"])

    def test_the_runner_reaches_the_work_function(self):
        job = self._job(AUDIT_REPORTS["primary_ip"], {}, status="running")
        with patch("forward_netbox.jobs._audit_primary_ip_work") as work:
            AuditPrimaryIpJob(job).run()
        work.assert_called_once_with(job)
