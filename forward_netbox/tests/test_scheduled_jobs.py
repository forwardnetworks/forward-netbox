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

    def test_validation_interval_without_schedule_at_defaults_to_now(self):
        with patch(
            "forward_netbox.jobs.ValidationJob.enqueue_once",
            return_value=Mock(pk=11),
        ) as once:
            enqueue_validation_job(self.sync, interval=720)
        self.assertIsNotNone(once.call_args.kwargs["schedule_at"])

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
