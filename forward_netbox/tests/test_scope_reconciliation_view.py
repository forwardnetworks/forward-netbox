"""Loading the scope reconciliation page must never query Forward.

`compute_scope_reconciliation` issues two live NQE `fetch_all` queries — every
device and every endpoint in the network — and the view called it inline on GET.
On a real fabric that exceeded the gateway timeout: a customer reported a 504
after about 30 seconds with the queries still running. It also meant every page
load hit Forward, which is the call volume Forward engineering asked us to cut.

The report is now computed by a background job and the page renders the stored
result, mirroring the dependency preview.
"""

from unittest.mock import patch
from uuid import uuid4

from core.choices import JobStatusChoices
from core.models import Job
from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.test import Client
from django.test import TestCase
from django.urls import reverse

from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync


class ScopeReconciliationViewTest(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="scope-src",
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
            name="scope-sync",
            source=self.source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        user = get_user_model().objects.create_user(
            username="scope-admin", password="x"
        )
        user.is_superuser = True
        user.is_staff = True
        user.save()
        self.client = Client()
        self.client.force_login(user)
        self.url = reverse(
            "plugins:forward_netbox:forwardsync_scope_reconciliation",
            kwargs={"pk": self.sync.pk},
        )

    def _store_report(self, data):
        return Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name=f"{self.sync.name} - scope reconciliation",
            status=JobStatusChoices.STATUS_COMPLETED,
            job_id=uuid4(),
            data=data,
        )

    def test_the_page_does_not_query_forward(self):
        # The whole point: a page load must not reach Forward at all.
        with patch(
            "forward_netbox.utilities.scope_reconciliation."
            "compute_scope_reconciliation"
        ) as compute:
            response = self.client.get(self.url)

        self.assertEqual(response.status_code, 200)
        compute.assert_not_called()

    def test_the_page_renders_before_any_report_exists(self):
        # Previously the first load *was* the computation; now it must render.
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.context["report_pending"])

    def test_a_stored_report_is_rendered(self):
        self._store_report({"scoped_device_count": 42})
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.context["report_pending"])
        self.assertEqual(response.context["payload"]["scoped_device_count"], 42)

    def test_a_failed_report_is_surfaced_not_swallowed(self):
        self._store_report(
            {
                "error": "Forward scope reconciliation failed (SyncError).",
                "error_type": "SyncError",
            }
        )
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, 200)
        self.assertIn("SyncError", response.context["report_error"])
        self.assertEqual(response.context["payload"], {})

    def test_the_newest_report_wins(self):
        self._store_report({"scoped_device_count": 1})
        self._store_report({"scoped_device_count": 2})
        response = self.client.get(self.url)
        self.assertEqual(response.context["payload"]["scoped_device_count"], 2)

    def test_refresh_enqueues_a_background_job(self):
        refresh_url = reverse(
            "plugins:forward_netbox:forwardsync_refresh_scope_reconciliation",
            kwargs={"pk": self.sync.pk},
        )
        # Patch where `jobs` bound the name, not where it is defined: the view
        # now goes through `ScopeReconciliationJob.enqueue`, and `jobs.py`
        # imports `enqueue_forward_job` at module load, so patching the source
        # module would not intercept the call.
        from forward_netbox.jobs import ScopeReconciliationJob

        with patch("forward_netbox.jobs.enqueue_forward_job") as enqueue:
            response = self.client.post(refresh_url)

        self.assertEqual(response.status_code, 302)
        enqueue.assert_called_once()
        # The bug was handing RQ a class instead of a callable, which it
        # rejects at dispatch - after the Job row is written. Pin the callable.
        # `handle` is a classmethod, so each access is a fresh bound method:
        # compare by equality, not identity.
        self.assertEqual(enqueue.call_args.args[0], ScopeReconciliationJob.handle)
