from datetime import timedelta

from django.test import TestCase
from django.utils import timezone

from forward_netbox.utilities.job_liveness import _started_job_heartbeat_stale


class JobLivenessTest(TestCase):
    def test_started_job_with_recent_heartbeat_is_not_stale(self):
        now = timezone.now()
        stale = _started_job_heartbeat_stale(
            status="started",
            last_heartbeat=now - timedelta(seconds=30),
            started_at=now - timedelta(seconds=120),
            now=now,
            threshold_seconds=180,
        )
        self.assertFalse(stale)

    def test_started_job_with_old_heartbeat_is_stale(self):
        now = timezone.now()
        stale = _started_job_heartbeat_stale(
            status="started",
            last_heartbeat=now - timedelta(seconds=600),
            started_at=now - timedelta(seconds=700),
            now=now,
            threshold_seconds=180,
        )
        self.assertTrue(stale)

    def test_non_started_status_is_never_stale(self):
        now = timezone.now()
        stale = _started_job_heartbeat_stale(
            status="finished",
            last_heartbeat=now - timedelta(seconds=9999),
            started_at=now - timedelta(seconds=9999),
            now=now,
            threshold_seconds=180,
        )
        self.assertFalse(stale)
