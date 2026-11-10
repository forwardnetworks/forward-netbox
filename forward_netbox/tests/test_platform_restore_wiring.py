# The operator surface around `platform_restore`: the two button jobs, the
# Scope Reconciliation card, the "What to do" item and the support bundle. The
# restore logic itself is `test_platform_restore`. Synthetic data only.
#
# Run: invoke test-isolated --test-label forward_netbox.tests.test_platform_restore_wiring
import json
import re
import uuid
from datetime import timedelta
from unittest.mock import patch

from core.choices import JobStatusChoices
from core.models import Job
from core.models import ObjectChange
from core.models import ObjectType
from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Platform
from dcim.models import Site
from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.contrib.messages import get_messages
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.platform_restore import PlatformRestoreRefused
from forward_netbox.utilities.platform_restore import preview_platform_restore
from forward_netbox.utilities.platform_restore import REFUSED_GREW_SINCE_PREVIEW
from forward_netbox.utilities.platform_restore import REFUSED_NO_PREVIEW
from forward_netbox.utilities.platform_restore import REFUSED_PREVIEW_STALE
from forward_netbox.utilities.platform_restore import REFUSED_SYNC_RUNNING
from forward_netbox.utilities.sync_facade import button_job_permission
from forward_netbox.utilities.sync_facade import enqueue_button_job
from forward_netbox.utilities.sync_facade import JobBlockedBySyncRun

CARD = "Platforms a Sync Run Blanked"
CHECKLIST_TITLE = "Platforms a sync run blanked on devices, VMs and config contexts"


class PlatformRestoreWiringFixture(TestCase):
    def setUp(self):
        self.admin = get_user_model().objects.create_superuser(
            username="plat-wiring-admin", password="TestPassword123!", email="a@b.c"
        )
        source = ForwardSource.objects.create(
            name="plat-wire-src",
            type="saas",
            url="https://forward.example.com",
            status="ready",
            parameters={"network_id": "net-1"},
        )
        self.sync = ForwardSync.objects.create(
            name="plat-wire-sync", source=source, parameters={}
        )
        self.request_id = uuid.uuid4()
        self.finalized_at = timezone.now()
        ForwardIngestion.objects.create(
            sync=self.sync,
            change_request_id=self.request_id,
            merge_finalized_at=self.finalized_at,
        )
        maker = Manufacturer.objects.create(name="Maker", slug="maker")
        self.site = Site.objects.create(name="Site", slug="site")
        self.role = DeviceRole.objects.create(name="Role", slug="role")
        self.device_type = DeviceType.objects.create(
            manufacturer=maker, model="Model", slug="model"
        )
        self.platform = Platform.objects.create(name="os-a", slug="os-a")
        self.device_ct = ObjectType.objects.get_for_model(Device)
        self.sync_ct = ContentType.objects.get_for_model(ForwardSync)

    def blanked_device(self, name):
        """A device whose platform one of this sync's own runs took away."""
        device = Device.objects.create(
            name=name,
            site=self.site,
            role=self.role,
            device_type=self.device_type,
            platform=self.platform,
        )
        change = ObjectChange.objects.create(
            request_id=self.request_id,
            user_name="forward",
            changed_object_type=self.device_ct,
            changed_object_id=device.pk,
            object_repr="x",
            action="update",
            prechange_data={"platform": self.platform.pk},
            postchange_data={"platform": None},
        )
        ObjectChange.objects.filter(pk=change.pk).update(
            time=self.finalized_at + timedelta(minutes=1)
        )
        Device.objects.filter(pk=device.pk).update(platform=None)
        return device

    def job(self, suffix, *, data=None, status=JobStatusChoices.STATUS_COMPLETED):
        return Job.objects.create(
            object_type=self.sync_ct,
            object_id=self.sync.pk,
            name=f"{self.sync.name} - {suffix}",
            status=status,
            job_id=uuid.uuid4(),
            user=self.admin,
            data=data,
        )

    def preview_job(self, data=None):
        """A completed preview job, computed now from the change log."""
        return self.job(
            "preview platform restore",
            data=preview_platform_restore(self.sync) if data is None else data,
        )

    def scope_page(self):
        self.client.force_login(self.admin)
        return self.client.get(
            reverse(
                "plugins:forward_netbox:forwardsync_scope_reconciliation",
                kwargs={"pk": self.sync.pk},
            )
        ).content.decode()


class PlatformRestoreJobTest(PlatformRestoreWiringFixture):
    def test_the_preview_job_stores_the_previews_counts_and_when_it_ran(self):
        from forward_netbox.jobs import _preview_platform_restore_work

        self.blanked_device("edge-1")
        job = self.job("preview platform restore", status="running")

        _preview_platform_restore_work(job)

        job.refresh_from_db()
        self.assertEqual(job.data["blanked_by_this_sync"], 1)
        self.assertEqual(job.data["restorable"], 1)
        self.assertEqual(job.data["held"], {})
        self.assertTrue(job.data["generated_at"])

    def test_the_restore_job_restores_what_the_preview_counted_as_the_invoking_user(
        self,
    ):
        from forward_netbox.jobs import _restore_platforms_work

        device = self.blanked_device("edge-1")
        preview = self.preview_job()
        job = self.job("restore platforms", status="running")

        _restore_platforms_work(job, preview_job_pk=preview.pk)

        job.refresh_from_db()
        device.refresh_from_db()
        self.assertEqual(device.platform_id, self.platform.pk)
        self.assertEqual(job.data["restored"], 1)
        self.assertEqual(job.data["preview_job"], preview.pk)
        self.assertTrue(job.data["finished_at"])
        change = (
            ObjectChange.objects.filter(
                changed_object_type=self.device_ct,
                changed_object_id=device.pk,
                postchange_data__platform=self.platform.pk,
            )
            .order_by("-time")
            .first()
        )
        self.assertIsNotNone(change)
        self.assertEqual(change.user_name, self.admin.username)

    def test_the_restore_job_with_no_named_preview_uses_the_latest_completed_one(self):
        from forward_netbox.jobs import _restore_platforms_work

        device = self.blanked_device("edge-1")
        self.preview_job()
        job = self.job("restore platforms", status="running")

        _restore_platforms_work(job)

        device.refresh_from_db()
        self.assertEqual(device.platform_id, self.platform.pk)

    def test_the_restore_job_refuses_without_a_completed_preview(self):
        from forward_netbox.jobs import _restore_platforms_work

        device = self.blanked_device("edge-1")
        # A preview that has not completed is not one the operator reviewed.
        self.job(
            "preview platform restore",
            data=preview_platform_restore(self.sync),
            status=JobStatusChoices.STATUS_RUNNING,
        )
        job = self.job("restore platforms", status="running")

        with self.assertRaises(PlatformRestoreRefused) as caught:
            _restore_platforms_work(job)

        self.assertEqual(caught.exception.reason, REFUSED_NO_PREVIEW)
        job.refresh_from_db()
        self.assertEqual(job.data["refused"], REFUSED_NO_PREVIEW)
        device.refresh_from_db()
        self.assertIsNone(device.platform_id)

    def test_the_restore_job_never_restores_more_than_the_preview_counted(self):
        from forward_netbox.jobs import _restore_platforms_work

        first = self.blanked_device("edge-1")
        self.preview_job()
        second = self.blanked_device("edge-2")
        job = self.job("restore platforms", status="running")

        with self.assertRaises(PlatformRestoreRefused) as caught:
            _restore_platforms_work(job)

        self.assertEqual(caught.exception.reason, REFUSED_GREW_SINCE_PREVIEW)
        for device in (first, second):
            device.refresh_from_db()
            self.assertIsNone(device.platform_id)

    def test_the_restore_job_refuses_a_preview_older_than_the_latest_run(self):
        from forward_netbox.jobs import _restore_platforms_work

        device = self.blanked_device("edge-1")
        data = preview_platform_restore(self.sync)
        data["generated_at"] = (self.finalized_at - timedelta(days=1)).isoformat()
        self.preview_job(data)
        job = self.job("restore platforms", status="running")

        with self.assertRaises(PlatformRestoreRefused) as caught:
            _restore_platforms_work(job)

        self.assertEqual(caught.exception.reason, REFUSED_PREVIEW_STALE)
        device.refresh_from_db()
        self.assertIsNone(device.platform_id)

    def test_the_restore_job_is_refused_while_a_sync_run_is_active(self):
        from forward_netbox.jobs import _restore_platforms_work

        device = self.blanked_device("edge-1")
        self.preview_job()
        self.job("adhoc", status=JobStatusChoices.STATUS_RUNNING)
        job = self.job("restore platforms", status="running")

        with self.assertRaises(PlatformRestoreRefused) as caught:
            _restore_platforms_work(job)

        self.assertEqual(caught.exception.reason, REFUSED_SYNC_RUNNING)
        job.refresh_from_db()
        self.assertEqual(job.data["refused"], REFUSED_SYNC_RUNNING)
        device.refresh_from_db()
        self.assertIsNone(device.platform_id)

    def test_the_restore_cannot_be_queued_while_a_sync_run_is_active(self):
        self.job("adhoc", status=JobStatusChoices.STATUS_RUNNING)

        with self.assertRaises(JobBlockedBySyncRun):
            enqueue_button_job(self.sync, "restore_platforms", None)

    def test_the_preview_can_be_queued_while_a_sync_run_is_active(self):
        self.job("adhoc", status=JobStatusChoices.STATUS_RUNNING)

        with patch("forward_netbox.jobs.enqueue_forward_job") as enqueue:
            enqueue_button_job(self.sync, "preview_platform_restore", None)

        enqueue.assert_called_once()

    def test_the_restore_needs_the_change_device_permission(self):
        self.assertEqual(
            button_job_permission("restore_platforms"), "dcim.change_device"
        )
        self.assertEqual(
            button_job_permission("preview_platform_restore"),
            "forward_netbox.run_forwardsync",
        )

    def test_the_restore_runner_forwards_the_named_preview_and_nothing_else(self):
        from forward_netbox.jobs import PreviewPlatformRestoreJob
        from forward_netbox.jobs import RestorePlatformsJob

        job = self.job("runner", status="running")
        with patch("forward_netbox.jobs._preview_platform_restore_work") as work:
            PreviewPlatformRestoreJob(job).run()
            work.assert_called_once_with(job)
        with patch("forward_netbox.jobs._restore_platforms_work") as work:
            RestorePlatformsJob(job).run()
            work.assert_called_once_with(job)
        with patch("forward_netbox.jobs._restore_platforms_work") as work:
            RestorePlatformsJob(job).run(preview_job_pk=4)
            work.assert_called_once_with(job, preview_job_pk=4)


class PlatformRestoreViewTest(PlatformRestoreWiringFixture):
    def post(self, name, data=None):
        self.client.force_login(self.admin)
        return self.client.post(
            reverse(
                f"plugins:forward_netbox:forwardsync_{name}",
                kwargs={"pk": self.sync.pk},
            ),
            data or {},
        )

    def test_the_restore_view_queues_nothing_without_a_preview(self):
        self.blanked_device("edge-1")

        with patch("forward_netbox.jobs.enqueue_forward_job") as enqueue:
            response = self.post("restore_platforms")

        enqueue.assert_not_called()
        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.url.endswith("#platform-restore"))
        texts = [str(message) for message in get_messages(response.wsgi_request)]
        self.assertTrue(any(REFUSED_NO_PREVIEW in text for text in texts), texts)

    def test_the_restore_view_queues_the_job_against_the_preview_the_page_showed(self):
        self.blanked_device("edge-1")
        preview = self.preview_job()
        # Not an active job: an active one of this kind would make the view
        # answer "already queued" and enqueue nothing.
        queued = self.job("restore platforms", status=JobStatusChoices.STATUS_COMPLETED)

        with patch(
            "forward_netbox.jobs.enqueue_forward_job",
            return_value=queued,
        ) as enqueue:
            response = self.post("restore_platforms", {"preview_job": str(preview.pk)})

        enqueue.assert_called_once()
        self.assertEqual(enqueue.call_args.kwargs["preview_job_pk"], preview.pk)
        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.url.endswith("#platform-restore"))

    def test_the_preview_view_queues_the_preview_job(self):
        queued = self.job(
            "preview platform restore",
            data={},
            status=JobStatusChoices.STATUS_COMPLETED,
        )

        with patch(
            "forward_netbox.jobs.enqueue_forward_job",
            return_value=queued,
        ) as enqueue:
            response = self.post("preview_platform_restore")

        enqueue.assert_called_once()
        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.url.endswith("#platform-restore"))

    def test_a_user_without_the_change_device_permission_cannot_restore(self):
        self.preview_job()
        plain = get_user_model().objects.create_user(
            username="plat-wiring-plain", password="TestPassword123!"
        )
        self.client.force_login(plain)

        with patch("forward_netbox.jobs.enqueue_forward_job") as enqueue:
            response = self.client.post(
                reverse(
                    "plugins:forward_netbox:forwardsync_restore_platforms",
                    kwargs={"pk": self.sync.pk},
                )
            )

        enqueue.assert_not_called()
        self.assertEqual(response.status_code, 403)


class PlatformRestoreCardTest(PlatformRestoreWiringFixture):
    def test_with_no_preview_the_card_offers_only_the_preview(self):
        page = self.scope_page()

        self.assertIn(CARD, page)
        self.assertIn("preview-platform-restore", page)
        self.assertNotIn("restore-platforms", page)

    def test_a_preview_that_found_nothing_blanked_is_one_quiet_line(self):
        self.preview_job()

        page = self.scope_page()

        self.assertIn(
            "no device, virtual machine or config context had its platform blanked by this sync",
            page,
        )
        self.assertNotIn("restore-platforms", page)
        self.assertNotIn("Blanked by this sync</th>", page)
        self.assertNotIn(CHECKLIST_TITLE, page)

    def test_a_preview_with_restorable_devices_shows_its_figures_and_the_button(self):
        self.blanked_device("edge-1")
        self.blanked_device("edge-2")
        self.preview_job()

        page = self.scope_page()

        for label, value in (
            ("Blanked by this sync", 2),
            ("Restorable", 2),
            ("Held", 0),
        ):
            self.assertRegex(
                page,
                rf'<th[^>]*scope="row"[^>]*>\s*{label}\s*</th>\s*<td>{value}</td>',
            )
        self.assertIn("Restore 2 platforms", page)
        self.assertIn("restore-platforms", page)

    def test_a_held_device_is_counted_by_reason_and_not_offered_for_restore(self):
        device = self.blanked_device("edge-1")
        # Someone set a platform on it and cleared it again after the run.
        later = ObjectChange.objects.create(
            request_id=uuid.uuid4(),
            user_name="person",
            changed_object_type=self.device_ct,
            changed_object_id=device.pk,
            object_repr="x",
            action="update",
            prechange_data={"platform": self.platform.pk},
            postchange_data={"platform": None},
        )
        ObjectChange.objects.filter(pk=later.pk).update(
            time=self.finalized_at + timedelta(minutes=5)
        )
        self.preview_job()

        page = self.scope_page()

        self.assertRegex(
            page, r'<th[^>]*scope="row"[^>]*>\s*Restorable\s*</th>\s*<td>0</td>'
        )
        self.assertRegex(page, r'<th[^>]*scope="row"[^>]*>\s*Held\s*</th>\s*<td>1</td>')
        self.assertIn("platform_changed_by_someone_else_since", page)
        self.assertNotIn("restore-platforms", page)


class PlatformRestoreBundleTest(PlatformRestoreWiringFixture):
    def bundle_section(self):
        self.client.force_login(self.admin)
        payload = json.loads(
            self.client.get(
                reverse(
                    "plugins:forward_netbox:forwardsync_support_bundle",
                    kwargs={"pk": self.sync.pk},
                )
            ).content
        )
        return payload["scope_reconciliation"]["platform_restore"]

    def test_the_bundle_carries_the_cards_figures_under_the_cards_labels(self):
        from forward_netbox.views import PLATFORM_RESTORE_CARD
        from forward_netbox.views import PLATFORM_RESTORE_FIGURES

        self.blanked_device("edge-1")
        self.blanked_device("edge-2")
        preview = self.preview_job()
        page = self.scope_page()

        section = self.bundle_section()

        self.assertEqual(PLATFORM_RESTORE_CARD, CARD)
        figures = {row["label"]: row for row in section["ui_figures"]}
        for label, _key in PLATFORM_RESTORE_FIGURES:
            self.assertIn(label, figures)
            # The value the card shows is the value the bundle holds.
            shown = re.search(
                rf'<th[^>]*scope="row"[^>]*>\s*{label}\s*</th>\s*<td>(\d+)</td>',
                page,
            )
            self.assertIsNotNone(shown, label)
            self.assertEqual(int(shown.group(1)), figures[label]["value"], label)
            self.assertEqual(figures[label]["card"], CARD)
            self.assertEqual(figures[label]["page"], "Scope Reconciliation")
            self.assertEqual(
                figures[label]["computed_at"], preview.data["generated_at"], label
            )
        self.assertEqual(figures["Blanked by this sync"]["value"], 2)
        self.assertEqual(figures["Restorable"]["value"], 2)
        self.assertEqual(section["preview_job"], preview.pk)

    def test_the_bundle_carries_the_hold_split_under_the_cards_reasons(self):
        preview = self.preview_job(
            {
                "blanked_by_this_sync": 5,
                "restorable": 2,
                "held": {
                    "platform_not_found_again": 2,
                    "several_platforms_with_that_slug": 1,
                },
                "generated_at": timezone.now().isoformat(),
            }
        )

        section = self.bundle_section()

        figures = {row["label"]: row for row in section["ui_figures"]}
        self.assertEqual(figures["Held"]["value"], 3)
        self.assertEqual(figures["Held: platform_not_found_again"]["value"], 2)
        self.assertEqual(figures["Held: several_platforms_with_that_slug"]["value"], 1)
        self.assertEqual(
            figures["Held: platform_not_found_again"]["computed_at"],
            preview.data["generated_at"],
        )

    def test_the_bundle_carries_the_last_restore_and_when_it_finished(self):
        finished = timezone.now().isoformat()
        self.preview_job()
        self.job("restore platforms", data={"restored": 4, "finished_at": finished})

        section = self.bundle_section()

        figures = {row["label"]: row for row in section["ui_figures"]}
        self.assertEqual(figures["Last restore ... restored"]["value"], 4)
        self.assertEqual(figures["Last restore ... restored"]["computed_at"], finished)
        self.assertEqual(section["last_restore"]["restored"], 4)


class PlatformRestoreChecklistTest(PlatformRestoreWiringFixture):
    def test_the_item_is_in_what_to_do_only_when_the_preview_says_restorable(self):
        self.assertNotIn(CHECKLIST_TITLE, self.scope_page())

        # A preview that found nothing restorable adds no item.
        self.preview_job()
        self.assertNotIn(CHECKLIST_TITLE, self.scope_page())

        self.blanked_device("edge-1")
        self.preview_job()
        page = self.scope_page()

        self.assertIn(CHECKLIST_TITLE, page)
        self.assertIn("Press Preview", page)

    def test_the_support_bundle_checklist_carries_the_item_too(self):
        from forward_netbox.utilities.bundle_diagnostics import _cleanup_checklist

        self.assertNotIn(
            "platform_blanked_by_sync",
            [item["key"] for item in _cleanup_checklist(self.sync)["items"]],
        )
        self.blanked_device("edge-1")
        self.preview_job()

        items = {item["key"]: item for item in _cleanup_checklist(self.sync)["items"]}

        self.assertEqual(items["platform_blanked_by_sync"]["count"], 1)
