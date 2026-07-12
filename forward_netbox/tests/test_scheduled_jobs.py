# Chunk 3 of the 2.6 automation tranche: JobRunner port + standing schedules
# for dependency preview and validation. The load-bearing invariants:
#   - immediate runs keep the legacy per-sync job names (shims);
#   - standing schedules use the fixed JobRunner Meta.name so enqueue_once
#     dedup (cls.name + instance) yields one schedule per sync;
#   - the validation job stays bound to the SYNC (no object rebind) so
#     JobRunner recurrence re-enqueues against the right instance.
from datetime import datetime
from datetime import timezone as dt_timezone
from unittest.mock import Mock
from unittest.mock import patch

from core.choices import JobStatusChoices
from core.models import Job
from django.contrib.contenttypes.models import ContentType
from django.test import TestCase

from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.sync_facade import enqueue_preview_schedule
from forward_netbox.utilities.sync_facade import enqueue_validation_job

SCHEDULE_AT = datetime(2026, 7, 12, 6, 0, 0, tzinfo=dt_timezone.utc)


def _make_sync(name):
    source = ForwardSource.objects.create(
        name=f"{name}-src",
        type="saas",
        url="https://fwd.app",
        status="ready",
        parameters={
            "username": "user@example.com",
            "password": "secret",
            "verify": True,
            "network_id": "net-1",
        },
    )
    return ForwardSync.objects.create(
        name=name,
        source=source,
        parameters={"snapshot_id": "latestProcessed"},
    )


class JobRunnerNameTest(TestCase):
    def test_fixed_meta_names(self):
        # enqueue_once dedup keys on cls.name + instance; the preview name must
        # also satisfy the icontains "dependency preview" lookups in views.py.
        from forward_netbox.jobs import DependencyPreviewJob
        from forward_netbox.jobs import ValidationJob

        self.assertEqual(DependencyPreviewJob.name, "dependency preview")
        self.assertEqual(ValidationJob.name, "validation")


class ValidationWorkBindingTest(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.sync = _make_sync("sched-vald")

    def test_job_stays_bound_to_sync_and_exposes_run_id_via_data(self):
        # Pre-2.6 the work fn rebound job.object_type/object_id to the
        # validation run; under JobRunner recurrence that would re-enqueue
        # with instance=validation_run and silently re-target the schedule.
        job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="validation",
            status=JobStatusChoices.STATUS_RUNNING,
            job_id="123e4567-e89b-12d3-a456-426614174300",
        )
        from forward_netbox.jobs import _validate_forwardsync_work

        with patch("forward_netbox.jobs.ForwardValidationRunner") as runner, patch(
            "forward_netbox.jobs.SyncLogging"
        ), patch("forward_netbox.jobs.safe_save_job_data"), patch.object(
            ForwardSource, "get_client", return_value=Mock()
        ):
            runner.return_value.run_query_validation.return_value = Mock(pk=77)
            _validate_forwardsync_work(job)

        job.refresh_from_db()
        self.assertEqual(
            job.object_type,
            ContentType.objects.get_for_model(ForwardSync),
        )
        self.assertEqual(job.object_id, self.sync.pk)
        self.assertEqual(job.data["validation_run_id"], 77)


class ScheduleEnqueueTest(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.sync = _make_sync("sched-enq")

    def test_validation_schedule_routes_to_enqueue_once(self):
        with patch(
            "forward_netbox.jobs.ValidationJob.enqueue_once",
            return_value=Mock(pk=10),
        ) as once, patch("forward_netbox.utilities.sync_facade.Job.enqueue") as plain:
            enqueue_validation_job(self.sync, schedule_at=SCHEDULE_AT, interval=1440)
        plain.assert_not_called()
        kwargs = once.call_args.kwargs
        self.assertIs(kwargs["instance"], self.sync)
        self.assertEqual(kwargs["schedule_at"], SCHEDULE_AT)
        self.assertEqual(kwargs["interval"], 1440)
        self.assertEqual(kwargs["user"], self.sync.user)

    def test_validation_interval_without_schedule_at_passes_none(self):
        # schedule_at must pass through untouched: core enqueue_once only
        # treats a re-post as idempotent when schedule_at is falsy or matches
        # the existing row, so defaulting to now() would churn the schedule.
        # None + interval = run now, then recur.
        with patch(
            "forward_netbox.jobs.ValidationJob.enqueue_once",
            return_value=Mock(pk=11),
        ) as once:
            enqueue_validation_job(self.sync, interval=720)
        self.assertIsNone(once.call_args.kwargs["schedule_at"])
        self.assertEqual(once.call_args.kwargs["interval"], 720)

    def test_validation_without_schedule_keeps_legacy_path(self):
        # The immediate path must keep the per-sync name (dotted-path shim) —
        # not the fixed JobRunner name.
        with patch("forward_netbox.jobs.ValidationJob.enqueue_once") as once, patch(
            "forward_netbox.utilities.sync_facade.Job.enqueue",
            return_value=Mock(pk=12),
        ) as plain:
            enqueue_validation_job(self.sync, adhoc=True)
        once.assert_not_called()
        self.assertEqual(plain.call_args.kwargs["name"], "sched-enq - validation")

    def test_preview_schedule_routes_to_enqueue_once(self):
        with patch(
            "forward_netbox.jobs.DependencyPreviewJob.enqueue_once",
            return_value=Mock(pk=13),
        ) as once:
            enqueue_preview_schedule(self.sync, schedule_at=SCHEDULE_AT, interval=10080)
        kwargs = once.call_args.kwargs
        self.assertIs(kwargs["instance"], self.sync)
        self.assertEqual(kwargs["interval"], 10080)
        self.assertEqual(kwargs["user"], self.sync.user)


class ScheduleAPITest(TestCase):
    """REST schedule params on validate + dependency-preview actions."""

    @classmethod
    def setUpTestData(cls):
        from django.contrib.auth import get_user_model

        User = get_user_model()
        cls.admin = User.objects.create_superuser(
            username="sched_admin",
            password="TestPassword123!",
            email="sched_admin@example.com",
        )
        cls.plain_user = User.objects.create_user(
            username="sched_plain", password="TestPassword123!"
        )
        cls.sync = _make_sync("sched-api")

    def _post(self, user, action_name, data=None):
        from rest_framework.test import APIRequestFactory
        from rest_framework.test import force_authenticate

        from forward_netbox.api.views import ForwardSyncViewSet

        factory = APIRequestFactory()
        request = factory.post(
            f"/api/plugins/forward/sync/{self.sync.pk}/x/",
            data or {},
            format="json",
        )
        force_authenticate(request, user=user)
        view = ForwardSyncViewSet.as_view({"post": action_name})
        return view(request, pk=self.sync.pk)

    def _scheduled_job_row(self, name, suffix):
        return Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name=name,
            status=JobStatusChoices.STATUS_SCHEDULED,
            job_id=f"123e4567-e89b-12d3-a456-4266141743{suffix}",
        )

    def test_validate_with_interval_schedules(self):
        job_row = self._scheduled_job_row("validation", "01")
        with patch(
            "forward_netbox.jobs.ValidationJob.enqueue_once",
            return_value=job_row,
        ) as once:
            response = self._post(self.admin, "validate", {"interval": 1440})
        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.data["name"], "validation")
        once.assert_called_once()
        self.assertEqual(once.call_args.kwargs["interval"], 1440)

    def test_preview_with_interval_schedules(self):
        job_row = self._scheduled_job_row("dependency preview", "02")
        with patch(
            "forward_netbox.jobs.DependencyPreviewJob.enqueue_once",
            return_value=job_row,
        ) as once:
            response = self._post(self.admin, "dependency_preview", {"interval": 10080})
        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.data["name"], "dependency preview")
        once.assert_called_once()

    def test_preview_schedule_requires_permission(self):
        with patch("forward_netbox.jobs.DependencyPreviewJob.enqueue_once") as once:
            response = self._post(
                self.plain_user, "dependency_preview", {"interval": 1440}
            )
        self.assertEqual(response.status_code, 403)
        once.assert_not_called()

    def test_invalid_interval_is_400(self):
        for action_name in ("validate", "dependency_preview"):
            with self.subTest(action=action_name):
                response = self._post(self.admin, action_name, {"interval": 0})
                self.assertEqual(response.status_code, 400)

    def test_preview_without_schedule_still_uses_button_path(self):
        # Regression guard: empty body must fall through to the chunk-2
        # button-job path (legacy per-sync name), not the scheduler.
        job_row = self._scheduled_job_row("sched-api - dependency preview", "03")
        with patch(
            "forward_netbox.utilities.sync_facade.Job.enqueue",
            return_value=job_row,
        ) as plain, patch(
            "forward_netbox.jobs.DependencyPreviewJob.enqueue_once"
        ) as once:
            response = self._post(self.admin, "dependency_preview", {})
        self.assertEqual(response.status_code, 201)
        once.assert_not_called()
        plain.assert_called_once()


class CatchupGateNameScopingTest(TestCase):
    """Blocker regression (2.5.6): a permanently-SCHEDULED standing-schedule
    row must not suppress the snapshot catch-up decision forever."""

    @classmethod
    def setUpTestData(cls):
        cls.sync = _make_sync("sched-catchup")

    def _decision(self):
        from types import SimpleNamespace

        from forward_netbox.choices import ForwardSyncStatusChoices
        from forward_netbox.utilities.snapshot_freshness import (
            latest_processed_catchup_decision,
        )

        self.sync.status = ForwardSyncStatusChoices.COMPLETED
        self.sync.save(update_fields=["status"])
        client = SimpleNamespace(
            get_latest_processed_snapshot_id=Mock(return_value="snapshot-2")
        )
        return latest_processed_catchup_decision(
            self.sync, current_snapshot_id="snapshot-1", client=client
        )

    def _job(self, name, status, suffix):
        return Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name=name,
            status=status,
            job_id=f"123e4567-e89b-12d3-a456-4266141744{suffix}",
        )

    def test_standing_schedule_row_does_not_suppress_catchup(self):
        self._job("dependency preview", JobStatusChoices.STATUS_SCHEDULED, "01")
        self._job("validation", JobStatusChoices.STATUS_SCHEDULED, "02")
        decision = self._decision()
        self.assertTrue(decision["should_queue"])
        self.assertEqual(decision["reason"], "latest_processed_advanced")

    def test_pending_sync_run_still_suppresses_catchup(self):
        self._job("sched-catchup - adhoc", JobStatusChoices.STATUS_PENDING, "03")
        decision = self._decision()
        self.assertFalse(decision["should_queue"])
        self.assertEqual(decision["reason"], "active_job_exists")


class ImmediateValidationGuardTest(TestCase):
    """2.5.6: the immediate validate path gets the same overlap guard as the
    button jobs (was a bare Job.enqueue that stacked duplicates)."""

    @classmethod
    def setUpTestData(cls):
        cls.sync = _make_sync("sched-vguard")

    def _job(self, name, status, suffix):
        return Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name=name,
            status=status,
            job_id=f"123e4567-e89b-12d3-a456-4266141745{suffix}",
        )

    def test_pending_per_sync_validation_blocks(self):
        from forward_netbox.utilities.sync_facade import JobAlreadyActive

        self._job("sched-vguard - validation", JobStatusChoices.STATUS_PENDING, "01")
        with self.assertRaises(JobAlreadyActive):
            enqueue_validation_job(self.sync, adhoc=True)

    def test_running_standing_occurrence_blocks(self):
        from forward_netbox.utilities.sync_facade import JobAlreadyActive

        self._job("validation", JobStatusChoices.STATUS_RUNNING, "02")
        with self.assertRaises(JobAlreadyActive):
            enqueue_validation_job(self.sync, adhoc=True)

    def test_scheduled_standing_row_does_not_block(self):
        # The schedule row itself is permanently SCHEDULED; only a live
        # occurrence (pending/running) may block the immediate run.
        self._job("validation", JobStatusChoices.STATUS_SCHEDULED, "03")
        with patch(
            "forward_netbox.utilities.sync_facade.Job.enqueue",
            return_value=Mock(pk=30),
        ) as plain:
            enqueue_validation_job(self.sync, adhoc=True)
        plain.assert_called_once()


class OccurrenceSkipGuardTest(TestCase):
    """Reverse-direction overlap guard: a standing-schedule occurrence skips
    (instead of stacking) when an immediate equivalent is active, and stops
    its own recurrence when the sync was deleted."""

    @classmethod
    def setUpTestData(cls):
        cls.sync = _make_sync("sched-skip")

    def _occurrence(self, name, suffix):
        return Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name=name,
            status=JobStatusChoices.STATUS_RUNNING,
            interval=1440,
            job_id=f"123e4567-e89b-12d3-a456-4266141746{suffix}",
        )

    def test_skips_when_immediate_equivalent_is_active(self):
        from forward_netbox.jobs import _skip_if_immediate_equivalent_active

        Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="sched-skip - dependency preview",
            status=JobStatusChoices.STATUS_RUNNING,
            job_id="123e4567-e89b-12d3-a456-426614174699",
        )
        occurrence = self._occurrence("dependency preview", "01")
        self.assertTrue(
            _skip_if_immediate_equivalent_active(occurrence, "dependency preview")
        )
        occurrence.refresh_from_db()
        self.assertEqual(occurrence.data["skipped"], "immediate_equivalent_active")
        # Recurrence must continue: interval untouched.
        self.assertEqual(occurrence.interval, 1440)

    def test_runs_when_no_equivalent_is_active(self):
        from forward_netbox.jobs import _skip_if_immediate_equivalent_active

        occurrence = self._occurrence("dependency preview", "02")
        self.assertFalse(
            _skip_if_immediate_equivalent_active(occurrence, "dependency preview")
        )

    def test_stops_recurrence_when_sync_is_deleted(self):
        from forward_netbox.jobs import _skip_if_immediate_equivalent_active

        occurrence = self._occurrence("validation", "03")
        occurrence.object_id = self.sync.pk + 999999
        occurrence.save(update_fields=["object_id"])
        self.assertTrue(_skip_if_immediate_equivalent_active(occurrence, "validation"))
        occurrence.refresh_from_db()
        self.assertEqual(occurrence.data["skipped"], "sync_deleted")
        self.assertIsNone(occurrence.interval)


class SyncDeleteScheduleCleanupTest(TestCase):
    """pre_delete signal cancels enqueued/scheduled jobs through Job.delete()
    so no RQ scheduler entry survives the sync (zombie schedule)."""

    def test_scheduled_jobs_removed_on_sync_delete(self):
        sync = _make_sync("sched-del")
        job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=sync.pk,
            name="dependency preview",
            status=JobStatusChoices.STATUS_SCHEDULED,
            interval=1440,
            job_id="123e4567-e89b-12d3-a456-426614174700",
        )
        sync.delete()
        self.assertFalse(Job.objects.filter(pk=job.pk).exists())


class LegacyShimLifecycleTest(TestCase):
    """The dotted-path shims own start/terminate around the shared work fns;
    a regression here compounds into permanent 409s via the overlap guard."""

    @classmethod
    def setUpTestData(cls):
        cls.sync = _make_sync("sched-shim")

    def _job(self, name, suffix):
        return Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name=name,
            status=JobStatusChoices.STATUS_PENDING,
            job_id=f"123e4567-e89b-12d3-a456-4266141748{suffix}",
        )

    def test_validate_shim_completes(self):
        from forward_netbox.jobs import validate_forwardsync

        job = self._job("sched-shim - validation", "01")
        with patch("forward_netbox.jobs._validate_forwardsync_work"):
            validate_forwardsync(job)
        job.refresh_from_db()
        self.assertEqual(job.status, JobStatusChoices.STATUS_COMPLETED)

    def test_validate_shim_errored_and_reraises_unexpected(self):
        from forward_netbox.jobs import validate_forwardsync

        job = self._job("sched-shim - validation", "02")
        with patch(
            "forward_netbox.jobs._validate_forwardsync_work",
            side_effect=ValueError("boom"),
        ):
            with self.assertRaises(ValueError):
                validate_forwardsync(job)
        job.refresh_from_db()
        self.assertEqual(job.status, JobStatusChoices.STATUS_ERRORED)

    def test_preview_shim_swallows_sync_error(self):
        from core.exceptions import SyncError

        from forward_netbox.jobs import forward_dependency_preview

        job = self._job("sched-shim - dependency preview", "03")
        with patch(
            "forward_netbox.jobs._dependency_preview_work",
            side_effect=SyncError("expected"),
        ):
            forward_dependency_preview(job)
        job.refresh_from_db()
        self.assertEqual(job.status, JobStatusChoices.STATUS_ERRORED)


class JobRunnerRunInvokesWorkTest(TestCase):
    """run() must execute the shared work fn on self.job (only Meta.name was
    pinned before 2.5.6)."""

    @classmethod
    def setUpTestData(cls):
        cls.sync = _make_sync("sched-run")

    def _job(self, name, suffix):
        return Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name=name,
            status=JobStatusChoices.STATUS_RUNNING,
            job_id=f"123e4567-e89b-12d3-a456-4266141749{suffix}",
        )

    def test_preview_run_invokes_work(self):
        from forward_netbox.jobs import DependencyPreviewJob

        job = self._job("dependency preview", "01")
        with patch("forward_netbox.jobs._dependency_preview_work") as work:
            DependencyPreviewJob(job).run()
        work.assert_called_once_with(job)

    def test_validation_run_invokes_work(self):
        from forward_netbox.jobs import ValidationJob

        job = self._job("validation", "02")
        with patch("forward_netbox.jobs._validate_forwardsync_work") as work:
            ValidationJob(job).run()
        work.assert_called_once_with(job)


class ScheduleAPIValidationTest(TestCase):
    """2.5.6 API polish: reject foot-gun schedule bodies instead of silently
    doing something else."""

    @classmethod
    def setUpTestData(cls):
        from django.contrib.auth import get_user_model

        User = get_user_model()
        cls.admin = User.objects.create_superuser(
            username="schedv_admin",
            password="TestPassword123!",
            email="schedv_admin@example.com",
        )
        cls.sync = _make_sync("sched-apiv")

    def _post(self, action_name, data):
        from rest_framework.test import APIRequestFactory
        from rest_framework.test import force_authenticate

        from forward_netbox.api.views import ForwardSyncViewSet

        factory = APIRequestFactory()
        request = factory.post(
            f"/api/plugins/forward/sync/{self.sync.pk}/x/", data, format="json"
        )
        force_authenticate(request, user=self.admin)
        view = ForwardSyncViewSet.as_view({"post": action_name})
        return view(request, pk=self.sync.pk)

    def test_past_schedule_at_is_400(self):
        response = self._post(
            "validate",
            {"schedule_at": "2020-01-01T00:00:00Z", "interval": 1440},
        )
        self.assertEqual(response.status_code, 400)

    def test_schedule_at_without_interval_is_400(self):
        # One-shot delayed runs would occupy (and silently replace) the
        # standing schedule's enqueue_once dedup slot.
        response = self._post("validate", {"schedule_at": "2033-01-01T00:00:00Z"})
        self.assertEqual(response.status_code, 400)

    def test_preview_interval_below_floor_is_400(self):
        response = self._post("dependency_preview", {"interval": 30})
        self.assertEqual(response.status_code, 400)
        # validate has no such floor beyond >= 1
        job_row = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="validation",
            status=JobStatusChoices.STATUS_SCHEDULED,
            job_id="123e4567-e89b-12d3-a456-426614174910",
        )
        with patch(
            "forward_netbox.jobs.ValidationJob.enqueue_once",
            return_value=job_row,
        ):
            response = self._post("validate", {"interval": 30})
        self.assertEqual(response.status_code, 201)

    def test_schedule_keys_on_non_schedulable_action_are_400(self):
        with patch("forward_netbox.utilities.sync_facade.Job.enqueue") as enqueue:
            response = self._post("prune_orphans", {"interval": 1440})
        self.assertEqual(response.status_code, 400)
        self.assertIn("does not support scheduling", response.data["detail"])
        enqueue.assert_not_called()

    def test_duplicate_immediate_validate_is_409(self):
        Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="sched-apiv - validation",
            status=JobStatusChoices.STATUS_RUNNING,
            job_id="123e4567-e89b-12d3-a456-426614174911",
        )
        response = self._post("validate", {})
        self.assertEqual(response.status_code, 409)


class EnqueueOnceIntegrationTest(TestCase):
    """Real (unmocked) enqueue_once semantics — the invariant commit b52fa2f
    exists for. Uses a far-future schedule_at and deletes the row afterwards
    so no live RQ scheduler entry survives the test."""

    @classmethod
    def setUpTestData(cls):
        cls.sync = _make_sync("sched-real")

    def test_idempotent_repost_and_interval_replacement(self):
        from forward_netbox.utilities.sync_facade import (
            enqueue_preview_schedule,
        )

        far_future = datetime(2035, 1, 1, 6, 0, 0, tzinfo=dt_timezone.utc)
        j1 = j2 = j3 = None
        try:
            j1 = enqueue_preview_schedule(
                self.sync, schedule_at=far_future, interval=1440
            )
            j2 = enqueue_preview_schedule(
                self.sync, schedule_at=far_future, interval=1440
            )
            self.assertEqual(j1.pk, j2.pk)
            self.assertEqual(j1.name, "dependency preview")
            j3 = enqueue_preview_schedule(
                self.sync, schedule_at=far_future, interval=720
            )
            self.assertNotEqual(j3.pk, j1.pk)
            self.assertEqual(
                Job.objects.filter(
                    name="dependency preview",
                    status=JobStatusChoices.STATUS_SCHEDULED,
                ).count(),
                1,
            )
        finally:
            for job in (j3, j2, j1):
                if job is not None and Job.objects.filter(pk=job.pk).exists():
                    job.delete()
