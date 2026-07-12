# Pinning suite for the sync job's bespoke self-reschedule loop
# (jobs.sync_forwardsync finally block). Backlog recon verdict: do NOT
# converge this onto core JobRunner recurrence — core cannot replicate the
# completion-time cadence anchor, the sync.scheduled/interval model-field
# source of truth, or the name-scoped skip-guard without rebuilding the
# intent/guard apparatus. These tests lock the load-bearing semantics so a
# future "simplify onto JobRunner" attempt trips loudly.
from datetime import datetime
from datetime import timedelta
from datetime import timezone as dt_timezone
from unittest.mock import patch

from core.choices import JobStatusChoices
from core.models import Job
from django.contrib.contenttypes.models import ContentType
from django.test import TestCase

from forward_netbox.choices import ForwardSyncStatusChoices
from forward_netbox.jobs import sync_forwardsync
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync

T_END = datetime(2026, 8, 1, 12, 0, 0, tzinfo=dt_timezone.utc)


class SyncRescheduleAnchorTest(TestCase):
    @classmethod
    def setUpTestData(cls):
        from django.contrib.auth import get_user_model

        cls.user = get_user_model().objects.create_user(username="pin-user")
        cls.source = ForwardSource.objects.create(
            name="pin-src",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "network_id": "net-1",
            },
        )

    def _sync(self, name, *, interval=30, scheduled=None):
        sync = ForwardSync.objects.create(
            name=name,
            source=self.source,
            user=self.user,
            parameters={"snapshot_id": "latestProcessed"},
            interval=interval,
        )
        # Set scheduled via .update() to bypass ForwardSync.save()'s
        # enqueue-on-scheduled hook (which would spawn a real "- scheduled"
        # sync-run job and pollute the guard's view). Then drop any job rows
        # the create path may have produced so the test controls the job set.
        ForwardSync.objects.filter(pk=sync.pk).update(
            scheduled=scheduled or (T_END - timedelta(hours=1))
        )
        sync.refresh_from_db()
        sync.jobs.all().delete()
        return sync

    def _job(self, sync, suffix, *, started=None):
        return Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=sync.pk,
            name=f"{sync.name} - scheduled",
            status=JobStatusChoices.STATUS_RUNNING,
            started=started or (T_END - timedelta(minutes=45)),
            job_id=f"123e4567-e89b-12d3-a456-4266141770{suffix}",
        )

    def test_reschedule_anchor_is_completion_time_not_started(self):
        # THE cadence pin: next run = completion + interval (a guaranteed
        # rest interval), NOT started + interval like core JobRunner. A sync
        # that runs longer than its interval must not re-fire ~immediately.
        sync = self._sync("pin-anchor", interval=30)
        job = self._job(sync, "01")
        with patch("forward_netbox.jobs.local_now", return_value=T_END), patch(
            "forward_netbox.jobs.Job.enqueue"
        ), patch.object(ForwardSync, "sync", return_value=None):
            sync_forwardsync(job)
        sync.refresh_from_db()
        self.assertEqual(sync.scheduled, T_END + timedelta(minutes=30))

    def test_adhoc_run_never_reschedules(self):
        sync = self._sync("pin-adhoc", interval=30)
        job = self._job(sync, "02")
        original = sync.scheduled
        with patch("forward_netbox.jobs.local_now", return_value=T_END), patch(
            "forward_netbox.jobs.Job.enqueue"
        ), patch.object(ForwardSync, "sync", return_value=None):
            sync_forwardsync(job, adhoc=True)
        sync.refresh_from_db()
        self.assertEqual(sync.scheduled, original)

    def test_cleared_schedule_stops_the_chain(self):
        # Operator clears sync.scheduled mid-run: the finally's refresh_from_db
        # picks it up and the guard skips (chain stops).
        sync = self._sync("pin-cleared", interval=30)
        job = self._job(sync, "03")
        ForwardSync.objects.filter(pk=sync.pk).update(scheduled=None)
        with patch("forward_netbox.jobs.local_now", return_value=T_END), patch(
            "forward_netbox.jobs.Job.enqueue"
        ), patch.object(ForwardSync, "sync", return_value=None):
            sync_forwardsync(job)
        sync.refresh_from_db()
        self.assertIsNone(sync.scheduled)

    def test_skip_guard_ignores_standing_schedule_rows(self):
        # Standing preview/validation rows are permanently SCHEDULED and must
        # NOT satisfy the skip-guard (regression twin of the catch-up gate).
        sync = self._sync(
            "pin-standing", interval=30, scheduled=T_END + timedelta(days=1)
        )
        Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=sync.pk,
            name="validation",
            status=JobStatusChoices.STATUS_SCHEDULED,
            job_id="123e4567-e89b-12d3-a456-426614177010",
        )
        job = self._job(sync, "04")
        with patch("forward_netbox.jobs.local_now", return_value=T_END), patch(
            "forward_netbox.jobs.Job.enqueue"
        ), patch.object(ForwardSync, "sync", return_value=None):
            sync_forwardsync(job)
        sync.refresh_from_db()
        # Rescheduled (standing row did not trip the guard).
        self.assertEqual(sync.scheduled, T_END + timedelta(minutes=30))

    def test_failed_run_still_reschedules(self):
        # The chain must survive a failed run.
        from forward_netbox.exceptions import ForwardSyncError

        sync = self._sync("pin-failed", interval=30)
        job = self._job(sync, "05")
        with patch("forward_netbox.jobs.local_now", return_value=T_END), patch(
            "forward_netbox.jobs.Job.enqueue"
        ), patch.object(ForwardSync, "sync", side_effect=ForwardSyncError("boom")):
            sync_forwardsync(job)
        sync.refresh_from_db()
        self.assertEqual(sync.status, ForwardSyncStatusChoices.FAILED)
        self.assertEqual(sync.scheduled, T_END + timedelta(minutes=30))
