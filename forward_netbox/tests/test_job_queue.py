from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import Mock
from unittest.mock import patch
from uuid import uuid4

import django_rq
from core.choices import JobStatusChoices
from core.models import Job
from django.test import override_settings
from django.test import TestCase
from django.utils import timezone

from forward_netbox.utilities.health_checks import timeout_check
from forward_netbox.utilities.health_summary_blocks import runtime_summary
from forward_netbox.utilities.health_summary_blocks import throughput_summary
from forward_netbox.utilities.job_queue import _dispatch_persisted_job
from forward_netbox.utilities.job_queue import enqueue_forward_job
from forward_netbox.utilities.runtime_guidance import effective_forward_job_timeout
from forward_netbox.utilities.runtime_guidance import effective_merge_job_timeout
from forward_netbox.utilities.runtime_guidance import (
    MINIMUM_FORWARD_JOB_TIMEOUT_SECONDS,
)


def queued_test_job(job, marker=None):
    return job, marker


class ForwardJobQueueTest(TestCase):
    def test_effective_timeout_uses_minimum_without_valid_setting(self):
        for configured in (None, "invalid"):
            with self.subTest(configured=configured), override_settings(
                RQ_DEFAULT_TIMEOUT=configured
            ):
                self.assertEqual(
                    effective_forward_job_timeout(),
                    MINIMUM_FORWARD_JOB_TIMEOUT_SECONDS,
                )

    @override_settings(RQ_DEFAULT_TIMEOUT=300)
    def test_merge_timeout_scales_above_floor_for_large_branch(self):
        self.assertEqual(effective_merge_job_timeout(750_000), 75_000)

    @override_settings(RQ_DEFAULT_TIMEOUT=108000)
    def test_merge_timeout_preserves_larger_operator_setting(self):
        self.assertEqual(effective_merge_job_timeout(750_000), 108_000)

    @override_settings(RQ_DEFAULT_TIMEOUT=300)
    def test_real_rq_job_carries_forward_timeout_minimum(self):
        with self.captureOnCommitCallbacks(execute=True):
            job = enqueue_forward_job(
                queued_test_job,
                name="Forward real RQ timeout",
                queue_name="default",
            )

        rq_job = django_rq.get_queue("default").fetch_job(str(job.job_id))
        self.assertIsNotNone(rq_job)
        try:
            self.assertEqual(rq_job.timeout, MINIMUM_FORWARD_JOB_TIMEOUT_SECONDS)
        finally:
            rq_job.delete()

    @override_settings(RQ_DEFAULT_TIMEOUT=300)
    def test_real_rq_job_carries_larger_workload_timeout(self):
        with self.captureOnCommitCallbacks(execute=True):
            job = enqueue_forward_job(
                queued_test_job,
                name="Forward real workload timeout",
                queue_name="default",
                job_timeout=75_000,
            )

        rq_job = django_rq.get_queue("default").fetch_job(str(job.job_id))
        self.assertIsNotNone(rq_job)
        try:
            self.assertEqual(rq_job.timeout, 75_000)
        finally:
            rq_job.delete()

    @override_settings(RQ_DEFAULT_TIMEOUT=300)
    def test_real_scheduled_rq_job_carries_forward_timeout_minimum(self):
        scheduled = timezone.now() + timedelta(hours=1)
        with self.captureOnCommitCallbacks(execute=True):
            job = enqueue_forward_job(
                queued_test_job,
                name="Forward real scheduled RQ timeout",
                queue_name="default",
                schedule_at=scheduled,
            )

        queue = django_rq.get_queue("default")
        rq_job = queue.fetch_job(str(job.job_id))
        self.assertIsNotNone(rq_job)
        try:
            self.assertEqual(rq_job.timeout, MINIMUM_FORWARD_JOB_TIMEOUT_SECONDS)
        finally:
            queue.scheduled_job_registry.remove(rq_job, delete_job=True)

    @override_settings(RQ_DEFAULT_TIMEOUT=300)
    @patch("forward_netbox.utilities.job_queue.django_rq.get_queue")
    def test_dispatch_enforces_forward_job_timeout_minimum(self, get_queue):
        queue = Mock()
        queue.enqueue.return_value = "queued"
        get_queue.return_value = queue
        job = Job.objects.create(
            name="Forward timeout minimum",
            status=JobStatusChoices.STATUS_PENDING,
            job_id=uuid4(),
            queue_name="default",
        )

        result = _dispatch_persisted_job(
            job.pk,
            queued_test_job,
            {"marker": "preserved"},
        )

        self.assertEqual(result, "queued")
        queue.enqueue.assert_called_once_with(
            queued_test_job,
            job_id=str(job.job_id),
            job=job,
            marker="preserved",
            job_timeout=MINIMUM_FORWARD_JOB_TIMEOUT_SECONDS,
        )

    @override_settings(RQ_DEFAULT_TIMEOUT=10800)
    @patch("forward_netbox.utilities.job_queue.django_rq.get_queue")
    def test_dispatch_preserves_larger_operator_timeout(self, get_queue):
        queue = Mock()
        get_queue.return_value = queue
        job = Job.objects.create(
            name="Forward operator timeout",
            status=JobStatusChoices.STATUS_PENDING,
            job_id=uuid4(),
            queue_name="default",
        )

        _dispatch_persisted_job(job.pk, queued_test_job, {})

        self.assertEqual(queue.enqueue.call_args.kwargs["job_timeout"], 10800)

    @override_settings(RQ_DEFAULT_TIMEOUT=300)
    @patch("forward_netbox.utilities.job_queue.django_rq.get_queue")
    def test_dispatch_preserves_larger_workload_timeout(self, get_queue):
        queue = Mock()
        get_queue.return_value = queue
        job = Job.objects.create(
            name="Forward workload timeout",
            status=JobStatusChoices.STATUS_PENDING,
            job_id=uuid4(),
            queue_name="default",
        )

        _dispatch_persisted_job(
            job.pk,
            queued_test_job,
            {},
            job_timeout=75_000,
        )

        self.assertEqual(queue.enqueue.call_args.kwargs["job_timeout"], 75_000)

    @override_settings(RQ_DEFAULT_TIMEOUT=300)
    @patch("forward_netbox.utilities.job_queue.django_rq.get_queue")
    def test_scheduled_dispatch_enforces_forward_job_timeout_minimum(
        self,
        get_queue,
    ):
        queue = Mock()
        get_queue.return_value = queue
        scheduled = timezone.now()
        job = Job.objects.create(
            name="Forward scheduled timeout",
            status=JobStatusChoices.STATUS_SCHEDULED,
            scheduled=scheduled,
            job_id=uuid4(),
            queue_name="default",
        )

        _dispatch_persisted_job(job.pk, queued_test_job, {})

        queue.enqueue_at.assert_called_once_with(
            scheduled,
            queued_test_job,
            job_id=str(job.job_id),
            job=job,
            job_timeout=MINIMUM_FORWARD_JOB_TIMEOUT_SECONDS,
        )

    @override_settings(RQ_DEFAULT_TIMEOUT=300)
    def test_health_reports_configured_and_effective_timeout(self):
        sync = SimpleNamespace(source=SimpleNamespace(parameters={"timeout": 1200}))

        result = timeout_check(sync)

        self.assertEqual(result["status"], "pass")
        self.assertIn("RQ_DEFAULT_TIMEOUT is 300s", result["message"])
        self.assertIn("effective 7200s", result["message"])

    @override_settings(RQ_DEFAULT_TIMEOUT=300)
    def test_health_warns_when_source_exceeds_effective_timeout(self):
        sync = SimpleNamespace(source=SimpleNamespace(parameters={"timeout": 9000}))

        result = timeout_check(sync)

        self.assertEqual(result["status"], "warn")
        self.assertIn("Effective Forward job timeout is 7200s", result["message"])
        self.assertIn("source timeout of 9000s", result["message"])

    def test_health_summaries_distinguish_configured_and_effective_timeouts(self):
        sync = SimpleNamespace(
            source=SimpleNamespace(parameters={}),
            parameters={},
            auto_merge=False,
            get_max_changes_per_staging_item=Mock(return_value=10000),
            get_snapshot_id=Mock(return_value="latestProcessed"),
        )

        for configured, effective in ((300, 7200), (10800, 10800)):
            with self.subTest(configured=configured), override_settings(
                RQ_DEFAULT_TIMEOUT=configured
            ):
                runtime = runtime_summary(sync)
                throughput = throughput_summary(sync, None)

                self.assertEqual(runtime["rq_default_timeout_seconds"], configured)
                self.assertEqual(runtime["forward_job_timeout_seconds"], effective)
                self.assertEqual(throughput["worker_timeout_seconds"], configured)
                self.assertEqual(throughput["forward_job_timeout_seconds"], effective)
