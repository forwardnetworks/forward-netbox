# Chunk 3 of the 2.6 automation tranche: JobRunner port + standing schedules
# for dependency preview and validation. The load-bearing invariants:
#   - immediate runs use the same JobRunner classes with per-sync job names;
#   - standing schedules use the fixed JobRunner Meta.name so the serialized
#     scheduler yields one schedule per sync without deleting a running row;
#   - the validation job stays bound to the SYNC (no object rebind) so
#     JobRunner recurrence re-enqueues against the right instance.
from datetime import datetime
from datetime import timezone as dt_timezone
from unittest.mock import Mock
from unittest.mock import patch

from core.choices import JobStatusChoices
from core.models import Job
from django.contrib.contenttypes.models import ContentType
from django.db.models.deletion import ProtectedError
from django.test import TestCase
from django.test import TransactionTestCase

from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.sync_facade import enqueue_preview_schedule
from forward_netbox.utilities.sync_facade import enqueue_validation_job

SCHEDULE_AT = datetime(2026, 7, 12, 6, 0, 0, tzinfo=dt_timezone.utc)


def _make_sync(name):
    from django.contrib.auth import get_user_model

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
        user=get_user_model().objects.create_user(username=f"{name}-owner"),
        parameters={"snapshot_id": "latestProcessed"},
    )


class JobRunnerNameTest(TestCase):
    def test_fixed_meta_names(self):
        # The serialized scheduler keys on cls.name + instance; the preview
        # name must also satisfy the icontains lookups in views.py.
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

    def test_validation_schedule_routes_to_serialized_scheduler(self):
        with patch(
            "forward_netbox.utilities.sync_facade._enqueue_standing_job",
            return_value=Mock(pk=10),
        ) as standing, patch("forward_netbox.jobs.enqueue_forward_job") as plain:
            enqueue_validation_job(self.sync, schedule_at=SCHEDULE_AT, interval=1440)
        plain.assert_not_called()
        args, kwargs = standing.call_args
        self.assertEqual(args[0].name, "validation")
        self.assertIs(kwargs["sync"], self.sync)
        self.assertEqual(kwargs["schedule_at"], SCHEDULE_AT)
        self.assertEqual(kwargs["interval"], 1440)
        self.assertEqual(kwargs["user"], self.sync.user)

    def test_validation_interval_without_schedule_at_passes_none(self):
        # schedule_at must pass through untouched so a re-post remains
        # idempotent. None + interval = run now, then recur.
        with patch(
            "forward_netbox.utilities.sync_facade._enqueue_standing_job",
            return_value=Mock(pk=11),
        ) as standing:
            enqueue_validation_job(self.sync, interval=720)
        self.assertIsNone(standing.call_args.kwargs["schedule_at"])
        self.assertEqual(standing.call_args.kwargs["interval"], 720)

    def test_validation_without_schedule_uses_runner_with_per_sync_name(self):
        # The immediate path uses the same runner while keeping its distinct
        # per-sync name so it cannot replace a standing schedule row.
        with patch(
            "forward_netbox.utilities.sync_facade._enqueue_standing_job"
        ) as standing, patch(
            "forward_netbox.jobs.enqueue_forward_job",
            return_value=Mock(pk=12),
        ) as plain:
            enqueue_validation_job(self.sync, adhoc=True)
        standing.assert_not_called()
        self.assertEqual(plain.call_args.kwargs["name"], "sched-enq - validation")

    def test_preview_schedule_routes_to_serialized_scheduler(self):
        with patch(
            "forward_netbox.utilities.sync_facade._enqueue_standing_job",
            return_value=Mock(pk=13),
        ) as standing:
            enqueue_preview_schedule(self.sync, schedule_at=SCHEDULE_AT, interval=10080)
        args, kwargs = standing.call_args
        self.assertEqual(args[0].name, "dependency preview")
        self.assertIs(kwargs["sync"], self.sync)
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
        # Detach: no enqueued row may exist at request time (the action
        # answers 200 for an idempotent re-post); the instance stays usable
        # as the mock return value.
        Job.objects.filter(pk=job_row.pk).delete()
        with patch(
            "forward_netbox.utilities.sync_facade._enqueue_standing_job",
            return_value=job_row,
        ) as standing:
            response = self._post(self.admin, "validate", {"interval": 1440})
        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.data["name"], "validation")
        standing.assert_called_once()
        self.assertEqual(standing.call_args.kwargs["interval"], 1440)

    def test_preview_with_interval_schedules(self):
        job_row = self._scheduled_job_row("dependency preview", "02")
        Job.objects.filter(pk=job_row.pk).delete()
        with patch(
            "forward_netbox.utilities.sync_facade._enqueue_standing_job",
            return_value=job_row,
        ) as standing:
            response = self._post(self.admin, "dependency_preview", {"interval": 10080})
        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.data["name"], "dependency preview")
        standing.assert_called_once()

    def test_preview_schedule_requires_permission(self):
        with patch(
            "forward_netbox.utilities.sync_facade._enqueue_standing_job"
        ) as standing:
            response = self._post(
                self.plain_user, "dependency_preview", {"interval": 1440}
            )
        self.assertEqual(response.status_code, 403)
        standing.assert_not_called()

    def test_invalid_interval_is_400(self):
        for action_name in ("validate", "dependency_preview"):
            with self.subTest(action=action_name):
                # 0 is the cancel sentinel (valid); negatives are rejected.
                response = self._post(self.admin, action_name, {"interval": -5})
                self.assertEqual(response.status_code, 400)

    def test_preview_without_schedule_still_uses_button_path(self):
        # Regression guard: empty body must use the immediate runner path with
        # a per-sync name, not the standing scheduler.
        job_row = self._scheduled_job_row("sched-api - dependency preview", "03")
        with patch(
            "forward_netbox.jobs.enqueue_forward_job",
            return_value=job_row,
        ) as plain, patch(
            "forward_netbox.utilities.sync_facade._enqueue_standing_job"
        ) as standing:
            response = self._post(self.admin, "dependency_preview", {})
        self.assertEqual(response.status_code, 201)
        standing.assert_not_called()
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
            "forward_netbox.jobs.enqueue_forward_job",
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
        cls.sync.parameters = {
            **cls.sync.parameters,
            "validation_schedule_interval": 1440,
            "preview_schedule_interval": 1440,
        }
        cls.sync.save()

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

    def test_running_job_protects_sync_from_deletion(self):
        sync = _make_sync("sched-del-running")
        job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=sync.pk,
            name="validation",
            status=JobStatusChoices.STATUS_RUNNING,
            interval=1440,
            started=datetime.now(tz=dt_timezone.utc),
            job_id="123e4567-e89b-12d3-a456-426614174701",
        )

        with self.assertRaises(ProtectedError):
            sync.delete()

        self.assertTrue(ForwardSync.objects.filter(pk=sync.pk).exists())
        self.assertTrue(Job.objects.filter(pk=job.pk).exists())

    def test_running_ingestion_merge_job_protects_sync_from_deletion(self):
        sync = _make_sync("sched-del-merge-running")
        ingestion = ForwardIngestion.objects.create(sync=sync)
        job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardIngestion),
            object_id=ingestion.pk,
            name="ingestion merge",
            status=JobStatusChoices.STATUS_RUNNING,
            started=datetime.now(tz=dt_timezone.utc),
            job_id="123e4567-e89b-12d3-a456-426614174702",
        )
        ingestion.merge_job = job
        ingestion.save(update_fields=["merge_job"])

        with self.assertRaises(ProtectedError):
            sync.delete()

        self.assertTrue(ForwardSync.objects.filter(pk=sync.pk).exists())
        self.assertTrue(ForwardIngestion.objects.filter(pk=ingestion.pk).exists())
        self.assertTrue(Job.objects.filter(pk=job.pk).exists())


class JobRunnerHandleLifecycleTest(TestCase):
    """Immediate jobs use NetBox's JobRunner lifecycle end to end."""

    @classmethod
    def setUpTestData(cls):
        cls.sync = _make_sync("sched-handle")

    def _job(self, name, suffix, *, interval=None):
        return Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name=name,
            status=JobStatusChoices.STATUS_PENDING,
            interval=interval,
            job_id=f"123e4567-e89b-12d3-a456-4266141748{suffix}",
        )

    def test_validation_runner_completes(self):
        from forward_netbox.jobs import ValidationJob

        job = self._job("sched-handle - validation", "01")
        with patch("forward_netbox.jobs._validate_forwardsync_work"):
            ValidationJob.handle(job)
        job.refresh_from_db()
        self.assertEqual(job.status, JobStatusChoices.STATUS_COMPLETED)

    def test_validation_runner_records_unexpected_error(self):
        from forward_netbox.jobs import ValidationJob

        job = self._job("sched-handle - validation", "02")
        with patch(
            "forward_netbox.jobs._validate_forwardsync_work",
            side_effect=ValueError("boom"),
        ):
            ValidationJob.handle(job)
        job.refresh_from_db()
        self.assertEqual(job.status, JobStatusChoices.STATUS_ERRORED)
        self.assertIn("boom", job.error)

    def test_preview_runner_records_sync_error(self):
        from core.exceptions import SyncError

        from forward_netbox.jobs import DependencyPreviewJob

        job = self._job("sched-handle - dependency preview", "03")
        with patch(
            "forward_netbox.jobs._dependency_preview_work",
            side_effect=SyncError("expected"),
        ):
            DependencyPreviewJob.handle(job)
        job.refresh_from_db()
        self.assertEqual(job.status, JobStatusChoices.STATUS_ERRORED)

    def test_recovered_validation_occurrence_reconciles_one_successor(self):
        from forward_netbox.jobs import ValidationJob
        from forward_netbox.utilities.stuck_recovery import _terminal_mark

        parameters = {
            **self.sync.parameters,
            "validation_schedule_interval": 30,
        }
        ForwardSync.objects.filter(pk=self.sync.pk).update(parameters=parameters)
        self.sync.parameters = parameters
        job = self._job("validation", "04", interval=30)
        stale_job = Job.objects.get(pk=job.pk)
        _terminal_mark([job.pk])

        with patch("forward_netbox.jobs._validate_forwardsync_work") as work:
            ValidationJob.handle(stale_job)
            ValidationJob.handle(stale_job)

        work.assert_not_called()
        successors = self.sync.jobs.filter(
            name="validation",
            status__in=JobStatusChoices.ENQUEUED_STATE_CHOICES,
        ).exclude(pk=job.pk)
        self.assertEqual(successors.count(), 1)
        self.assertEqual(successors.get().interval, 30)


class JobRunnerRunInvokesWorkTest(TestCase):
    """run() executes the shared work function on the bound job."""

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
        # One-shot delayed runs would occupy the standing schedule slot.
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
        Job.objects.filter(pk=job_row.pk).delete()
        with patch(
            "forward_netbox.utilities.sync_facade._enqueue_standing_job",
            return_value=job_row,
        ):
            response = self._post("validate", {"interval": 30})
        self.assertEqual(response.status_code, 201)

    def test_schedule_keys_on_non_schedulable_action_are_400(self):
        with patch("forward_netbox.jobs.enqueue_forward_job") as enqueue:
            response = self._post("prune_orphans", {"interval": 1440})
        self.assertEqual(response.status_code, 400)
        self.assertIn("does not support scheduling", response.data["detail"])
        enqueue.assert_not_called()

    def test_duplicate_immediate_validate_is_202_already_running(self):
        Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="sched-apiv - validation",
            status=JobStatusChoices.STATUS_RUNNING,
            job_id="123e4567-e89b-12d3-a456-426614174911",
        )
        response = self._post("validate", {})
        self.assertEqual(response.status_code, 202)
        self.assertEqual(response.data["status"], "already_running")


class StandingScheduleIntegrationTest(TransactionTestCase):
    """Real serialized standing-schedule semantics.

    Uses a far-future schedule_at and deletes the row afterwards so no live RQ
    scheduler entry survives the test.
    """

    def setUp(self):
        self.sync = _make_sync("sched-real")

    def test_idempotent_repost_and_interval_replacement(self):
        from forward_netbox.utilities.job_liveness import _rq_job_is_active
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
            self.assertIs(_rq_job_is_active(j1), True)
            j3 = enqueue_preview_schedule(
                self.sync, schedule_at=far_future, interval=720
            )
            self.assertNotEqual(j3.pk, j1.pk)
            self.assertFalse(Job.objects.filter(pk=j1.pk).exists())
            self.assertIs(_rq_job_is_active(j1), False)
            self.assertIs(_rq_job_is_active(j3), True)
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
            if j3 is not None:
                self.assertIs(_rq_job_is_active(j3), False)


class DesiredStateScheduleTest(TestCase):
    """2.5.7: schedule intent lives in sync.parameters; reconcile makes Job
    rows match it (form save + end-of-sync self-heal)."""

    @classmethod
    def setUpTestData(cls):
        cls.sync = _make_sync("sched-intent")

    def test_api_schedule_persists_intent(self):
        with patch(
            "forward_netbox.utilities.sync_facade._enqueue_standing_job",
            return_value=Mock(pk=40),
        ):
            enqueue_preview_schedule(self.sync, interval=1440)
        self.sync.refresh_from_db()
        self.assertEqual(self.sync.parameters["preview_schedule_interval"], 1440)

    def test_cancel_clears_intent_and_rows(self):
        from forward_netbox.utilities.sync_facade import (
            cancel_standing_schedule,
        )

        self.sync.parameters = {
            **(self.sync.parameters or {}),
            "preview_schedule_interval": 1440,
        }
        self.sync.save()
        row = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="dependency preview",
            status=JobStatusChoices.STATUS_SCHEDULED,
            interval=1440,
            job_id="123e4567-e89b-12d3-a456-426614175001",
        )
        removed = cancel_standing_schedule(self.sync, "dependency_preview")
        self.assertEqual(removed, 1)
        self.assertFalse(Job.objects.filter(pk=row.pk).exists())
        self.sync.refresh_from_db()
        # Cancel stores the canonical disabled intent.
        self.assertEqual(self.sync.parameters["preview_schedule_interval"], 0)

    def test_reconcile_recreates_missing_schedule(self):
        # Self-heal: intent stored but no enqueued row (e.g. worker was
        # hard-killed mid-occurrence) -> reconcile recreates the chain.
        from forward_netbox.utilities.sync_facade import (
            reconcile_standing_schedules,
        )

        self.sync.parameters = {
            **(self.sync.parameters or {}),
            "validation_schedule_interval": 720,
        }
        self.sync.save()
        with patch(
            "forward_netbox.utilities.sync_facade._enqueue_standing_job",
            return_value=Mock(pk=41),
        ) as standing:
            reconcile_standing_schedules(self.sync)
        standing.assert_called_once()
        self.assertEqual(standing.call_args.kwargs["interval"], 720)

    def test_reconcile_removes_rows_with_cancelled_intent(self):
        from forward_netbox.utilities.sync_facade import (
            reconcile_standing_schedules,
        )

        self.sync.parameters = {
            **(self.sync.parameters or {}),
            "validation_schedule_interval": 0,
        }
        self.sync.save()
        row = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="validation",
            status=JobStatusChoices.STATUS_SCHEDULED,
            interval=60,
            job_id="123e4567-e89b-12d3-a456-426614175002",
        )
        reconcile_standing_schedules(self.sync)
        self.assertFalse(Job.objects.filter(pk=row.pk).exists())


class ScheduleCancelAPITest(TestCase):
    """interval=0 cancels the standing schedule via the API."""

    @classmethod
    def setUpTestData(cls):
        from django.contrib.auth import get_user_model

        User = get_user_model()
        cls.admin = User.objects.create_superuser(
            username="cancel_admin",
            password="TestPassword123!",
            email="cancel_admin@example.com",
        )
        cls.sync = _make_sync("sched-cancel")

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

    def test_interval_zero_cancels(self):
        self.sync.parameters = {
            **(self.sync.parameters or {}),
            "validation_schedule_interval": 1440,
        }
        self.sync.save()
        row = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="validation",
            status=JobStatusChoices.STATUS_SCHEDULED,
            interval=1440,
            job_id="123e4567-e89b-12d3-a456-426614175101",
        )
        response = self._post("validate", {"interval": 0})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["status"], "cancelled")
        self.assertEqual(response.data["removed"], 1)
        self.assertFalse(Job.objects.filter(pk=row.pk).exists())
        self.sync.refresh_from_db()
        self.assertEqual(self.sync.parameters["validation_schedule_interval"], 0)

    def test_interval_zero_with_schedule_at_is_400(self):
        response = self._post(
            "validate", {"interval": 0, "schedule_at": "2033-01-01T00:00:00Z"}
        )
        self.assertEqual(response.status_code, 400)

    def test_idempotent_repost_returns_200(self):
        row = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="validation",
            status=JobStatusChoices.STATUS_SCHEDULED,
            interval=1440,
            job_id="123e4567-e89b-12d3-a456-426614175102",
        )
        with patch(
            "forward_netbox.utilities.sync_facade._enqueue_standing_job",
            return_value=row,
        ):
            response = self._post("validate", {"interval": 1440})
        self.assertEqual(response.status_code, 200)

    def test_new_schedule_returns_201(self):
        row = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="validation",
            status=JobStatusChoices.STATUS_SCHEDULED,
            interval=720,
            job_id="123e4567-e89b-12d3-a456-426614175103",
        )
        # No enqueued row exists at request time (deleted from the DB; the
        # detached instance stays usable as the mock's return value).
        Job.objects.filter(pk=row.pk).delete()
        with patch(
            "forward_netbox.utilities.sync_facade._enqueue_standing_job",
            return_value=row,
        ):
            response = self._post("validate", {"interval": 720})
        self.assertEqual(response.status_code, 201)


class ValidationRunRetentionTest(TestCase):
    """Recurring validation must not accumulate runs forever."""

    @classmethod
    def setUpTestData(cls):
        cls.sync = _make_sync("sched-retain")

    def test_trim_keeps_newest_n(self):
        from forward_netbox.jobs import _trim_validation_runs
        from forward_netbox.models import ForwardValidationRun

        all_pks = [
            ForwardValidationRun.objects.create(sync=self.sync).pk for _i in range(7)
        ]
        with patch(
            "forward_netbox.choices.forward_plugin_settings",
            return_value={"validation_run_retention": 5},
        ):
            _trim_validation_runs(self.sync)
        remaining = set(
            ForwardValidationRun.objects.filter(sync=self.sync).values_list(
                "pk", flat=True
            )
        )
        # The NEWEST five survive (a flipped ordering would keep the oldest).
        self.assertEqual(remaining, set(sorted(all_pks, reverse=True)[:5]))

    def test_zero_disables_trim(self):
        from forward_netbox.jobs import _trim_validation_runs
        from forward_netbox.models import ForwardValidationRun

        for _i in range(3):
            ForwardValidationRun.objects.create(sync=self.sync)
        with patch(
            "forward_netbox.choices.forward_plugin_settings",
            return_value={"validation_run_retention": 0},
        ):
            _trim_validation_runs(self.sync)
        self.assertEqual(ForwardValidationRun.objects.filter(sync=self.sync).count(), 3)


class OccurrenceIntentGuardTest(TestCase):
    """The occurrence re-reads the stored intent at each firing: cancelled
    (0) stops the chain and a changed interval re-aligns it. This makes a
    cancel/replace racing a RUNNING occurrence self-terminate."""

    @classmethod
    def setUpTestData(cls):
        cls.sync = _make_sync("sched-intent2")

    def _occurrence(self, name, interval, suffix):
        return Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name=name,
            status=JobStatusChoices.STATUS_RUNNING,
            interval=interval,
            job_id=f"123e4567-e89b-12d3-a456-4266141752{suffix}",
        )

    def _set_intent(self, key, value):
        params = dict(self.sync.parameters or {})
        params[key] = value
        self.sync.parameters = params
        self.sync.save()

    def test_cancelled_intent_stops_the_chain(self):
        from forward_netbox.jobs import _skip_if_immediate_equivalent_active

        self._set_intent("validation_schedule_interval", 0)
        occurrence = self._occurrence("validation", 720, "01")
        self.assertTrue(_skip_if_immediate_equivalent_active(occurrence, "validation"))
        occurrence.refresh_from_db()
        self.assertIsNone(occurrence.interval)
        self.assertEqual(occurrence.data["skipped"], "schedule_cancelled")

    def test_changed_intent_realigns_the_chain(self):
        from forward_netbox.jobs import _skip_if_immediate_equivalent_active

        self._set_intent("validation_schedule_interval", 1440)
        occurrence = self._occurrence("validation", 720, "02")
        self.assertFalse(_skip_if_immediate_equivalent_active(occurrence, "validation"))
        occurrence.refresh_from_db()
        self.assertEqual(occurrence.interval, 1440)

    def test_missing_intent_fails_closed_and_stops_the_chain(self):
        from forward_netbox.jobs import _skip_if_immediate_equivalent_active

        parameters = dict(self.sync.parameters)
        parameters.pop("preview_schedule_interval", None)
        ForwardSync.objects.filter(pk=self.sync.pk).update(parameters=parameters)
        occurrence = self._occurrence("dependency preview", 1440, "03")
        self.assertTrue(
            _skip_if_immediate_equivalent_active(occurrence, "dependency preview")
        )
        occurrence.refresh_from_db()
        self.assertIsNone(occurrence.interval)
        self.assertEqual(occurrence.data["skipped"], "schedule_cancelled")


class ReconcileCanonicalIntentTest(TestCase):
    """Runtime reconciliation follows canonical persisted intent only."""

    @classmethod
    def setUpTestData(cls):
        cls.sync = _make_sync("sched-canonical")

    def test_disabled_intent_removes_unclaimed_schedule_row(self):
        from forward_netbox.utilities.sync_facade import (
            reconcile_standing_schedules,
        )

        row = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="validation",
            status=JobStatusChoices.STATUS_SCHEDULED,
            interval=1440,
            job_id="123e4567-e89b-12d3-a456-426614175301",
        )
        reconcile_standing_schedules(self.sync)
        self.assertFalse(Job.objects.filter(pk=row.pk).exists())

    def test_validation_api_schedule_persists_intent(self):
        # The validation-kind twin of the preview persist test.
        with patch(
            "forward_netbox.utilities.sync_facade._enqueue_standing_job",
            return_value=Mock(pk=50),
        ):
            enqueue_validation_job(self.sync, interval=720)
        self.sync.refresh_from_db()
        self.assertEqual(self.sync.parameters["validation_schedule_interval"], 720)


class BlockedBySyncRunAPITest(TestCase):
    """Prune refused during a sync run answers a DISTINCT 202 status: the
    requested work is not queued (unlike already_running)."""

    @classmethod
    def setUpTestData(cls):
        from django.contrib.auth import get_user_model

        User = get_user_model()
        cls.admin = User.objects.create_superuser(
            username="blocked_admin",
            password="TestPassword123!",
            email="blocked_admin@example.com",
        )
        cls.sync = _make_sync("sched-blocked")

    def test_prune_during_sync_is_202_blocked(self):
        from rest_framework.test import APIRequestFactory
        from rest_framework.test import force_authenticate

        from forward_netbox.api.views import ForwardSyncViewSet

        Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="sched-blocked - adhoc",
            status=JobStatusChoices.STATUS_RUNNING,
            job_id="123e4567-e89b-12d3-a456-426614175401",
        )
        factory = APIRequestFactory()
        request = factory.post(
            f"/api/plugins/forward/sync/{self.sync.pk}/x/", {}, format="json"
        )
        force_authenticate(request, user=self.admin)
        view = ForwardSyncViewSet.as_view({"post": "prune_orphans"})
        response = view(request, pk=self.sync.pk)
        self.assertEqual(response.status_code, 202)
        self.assertEqual(response.data["status"], "blocked_by_sync_run")

    def test_interval_zero_on_prune_is_400(self):
        # Key-presence guard: 0 is falsy but must still be rejected.
        from rest_framework.test import APIRequestFactory
        from rest_framework.test import force_authenticate

        from forward_netbox.api.views import ForwardSyncViewSet

        factory = APIRequestFactory()
        request = factory.post(
            f"/api/plugins/forward/sync/{self.sync.pk}/x/",
            {"interval": 0},
            format="json",
        )
        force_authenticate(request, user=self.admin)
        view = ForwardSyncViewSet.as_view({"post": "prune_orphans"})
        response = view(request, pk=self.sync.pk)
        self.assertEqual(response.status_code, 400)


class SyncFormScheduleTest(TestCase):
    """The 2.5.7 form path: fields round-trip through parameters and save
    reconciles the Job rows."""

    @classmethod
    def setUpTestData(cls):
        cls.sync = _make_sync("sched-form")

    def _form_data(self, **overrides):
        data = {
            "name": self.sync.name,
            "source": self.sync.source.pk,
            "snapshot_id": "latestProcessed",
            "dcim.device": True,
        }
        data.update(overrides)
        return data

    def test_initial_from_parameters(self):
        from forward_netbox.forms import ForwardSyncForm

        self.sync.parameters = {
            **(self.sync.parameters or {}),
            "preview_schedule_interval": 1440,
        }
        self.sync.save()
        form = ForwardSyncForm(instance=self.sync)
        self.assertEqual(form.fields["preview_schedule_interval"].initial, 1440)

    def test_save_persists_intent_and_reconciles(self):
        from forward_netbox.forms import ForwardSyncForm

        with patch.object(self.sync.source.__class__, "get_client") as get_client:
            get_client.return_value.get_snapshots.return_value = []
            form = ForwardSyncForm(
                data=self._form_data(validation_schedule_interval=720),
                instance=self.sync,
            )
            self.assertTrue(form.is_valid(), form.errors)
            with patch(
                "forward_netbox.utilities.sync_facade." "reconcile_standing_schedules"
            ) as reconcile:
                instance = form.save()
        reconcile.assert_called_once_with(instance)
        self.assertEqual(instance.parameters["validation_schedule_interval"], 720)
        # Blank field = explicit cancel (stored 0, not absent).
        self.assertEqual(instance.parameters["preview_schedule_interval"], 0)


class DeviceCVETabTest(TestCase):
    """CVE device tab is registered only when netbox_dlm is installed (core
    installs must carry no dead tab); content is skip-gated on the plugin."""

    def test_registration_matches_plugin_presence(self):
        from django.apps import apps as django_apps

        import forward_netbox.views as views

        self.assertEqual(
            django_apps.is_installed("netbox_dlm"),
            hasattr(views, "ForwardDeviceCVEView"),
        )

    def test_tab_lists_device_cves(self):
        from django.apps import apps as django_apps

        if not django_apps.is_installed("netbox_dlm"):
            self.skipTest("netbox_dlm is not installed")
        from django.contrib.auth import get_user_model

        from dcim.models import Device
        from dcim.models import DeviceRole
        from dcim.models import DeviceType
        from dcim.models import Manufacturer
        from dcim.models import Site

        manufacturer = Manufacturer.objects.create(name="cve-mfr", slug="cve-mfr")
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer, model="cve-dt", slug="cve-dt"
        )
        role = DeviceRole.objects.create(name="cve-role", slug="cve-role")
        site = Site.objects.create(name="cve-site", slug="cve-site")
        device = Device.objects.create(
            name="cve-device",
            device_type=device_type,
            role=role,
            site=site,
        )
        from dcim.models import Platform

        CVE = django_apps.get_model("netbox_dlm", "cve")
        SoftwareVersion = django_apps.get_model("netbox_dlm", "softwareversion")
        Vulnerability = django_apps.get_model("netbox_dlm", "vulnerability")
        platform = Platform.objects.create(name="cve-plat", slug="cve-plat")
        # netbox_dlm.Vulnerability requires software_version (not null).
        software_version = SoftwareVersion.objects.create(
            platform=platform, version="1.0"
        )
        cve = CVE.objects.create(
            cve_id="CVE-2026-0001", name="test", severity="critical"
        )
        Vulnerability.objects.create(
            device=device, cve=cve, software_version=software_version
        )
        admin = get_user_model().objects.create_superuser(
            username="cve_admin", password="TestPassword123!"
        )
        self.client.force_login(admin)
        response = self.client.get(f"/dcim/devices/{device.pk}/forward-cves/")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"CVE-2026-0001", response.content)


class PatchIntentHookTest(TestCase):
    """REST PATCH of standing-schedule intent reconciles immediately."""

    @classmethod
    def setUpTestData(cls):
        from django.contrib.auth import get_user_model

        User = get_user_model()
        cls.admin = User.objects.create_superuser(
            username="patch_admin",
            password="TestPassword123!",
            email="patch_admin@example.com",
        )
        cls.sync = _make_sync("sched-patch")

    def _patch(self, data):
        from rest_framework.test import APIRequestFactory
        from rest_framework.test import force_authenticate

        from forward_netbox.api.views import ForwardSyncViewSet

        factory = APIRequestFactory()
        request = factory.patch(
            f"/api/plugins/forward/sync/{self.sync.pk}/", data, format="json"
        )
        force_authenticate(request, user=self.admin)
        view = ForwardSyncViewSet.as_view({"patch": "partial_update"})
        return view(request, pk=self.sync.pk)

    def test_intent_patch_triggers_reconcile(self):
        params = {
            **(self.sync.parameters or {}),
            "validation_schedule_interval": 720,
        }
        with patch(
            "forward_netbox.utilities.sync_facade.reconcile_standing_schedules"
        ) as reconcile:
            response = self._patch({"parameters": params})
        self.assertEqual(response.status_code, 200, response.data)
        reconcile.assert_called_once()

    def test_non_intent_patch_does_not_reconcile(self):
        with patch(
            "forward_netbox.utilities.sync_facade.reconcile_standing_schedules"
        ) as reconcile:
            response = self._patch({"name": "sched-patch"})
        self.assertEqual(response.status_code, 200, response.data)
        reconcile.assert_not_called()

    def test_display_parameters_echo_intent_keys(self):
        # GET-modify-PATCH round-trips retain canonical disabled intent.
        from forward_netbox.utilities.branch_budget import (
            DEFAULT_MAX_CHANGES_PER_STAGING_ITEM,
        )
        from forward_netbox.utilities.sync_state import get_display_parameters

        self.sync.parameters = {
            **(self.sync.parameters or {}),
            "preview_schedule_interval": 0,
        }
        self.sync.save()
        display = get_display_parameters(
            self.sync,
            max_changes_per_staging_item_default=DEFAULT_MAX_CHANGES_PER_STAGING_ITEM,
        )
        self.assertEqual(display["preview_schedule_interval"], 0)

    def test_persist_is_key_isolated_under_stale_instance(self):
        # Transactional persist reads the locked row, so a stale in-memory
        # instance cannot clobber the other kind's key.
        from forward_netbox.utilities.sync_facade import (
            persist_standing_schedule_interval,
        )

        stale = ForwardSync.objects.get(pk=self.sync.pk)
        persist_standing_schedule_interval(self.sync, "validation", 720)
        persist_standing_schedule_interval(stale, "dependency_preview", 1440)
        self.sync.refresh_from_db()
        self.assertEqual(self.sync.parameters["validation_schedule_interval"], 720)
        self.assertEqual(self.sync.parameters["preview_schedule_interval"], 1440)
