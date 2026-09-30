"""When Auto merge stops for review, and - as important - when it never does.

A guard that holds a healthy run gets switched off, and one that misses the
run it exists for is worse than none. These pin both sides against real syncs,
ingestions, jobs and rows: the customer's shape (a big run into an established
table) is held, and a first-ever run, a sync with Auto merge off, a small run
and an empty table are all left alone.
"""

from datetime import timedelta
from uuid import uuid4

from core.choices import JobStatusChoices
from core.models import Job
from dcim.models import Site
from django.contrib.contenttypes.models import ContentType
from django.test import TestCase
from django.utils import timezone

from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.run_size_anomaly import HOLD_JOB_DATA_KEY
from forward_netbox.utilities.run_size_anomaly import MIN_CHANGES
from forward_netbox.utilities.run_size_guard import decide_run_size_hold
from forward_netbox.utilities.run_size_guard import hold_record
from forward_netbox.utilities.run_size_guard import record_hold

SITE = "dcim.site"


class RecordingLogger:
    """The two things `record_hold` uses from the run's logger."""

    def __init__(self):
        self.log_data = {}
        self.flushed = 0

    def flush(self):
        self.flushed += 1


class GuardTestBase(TestCase):
    def setUp(self):
        source = ForwardSource.objects.create(
            name="guard-source",
            type="saas",
            url="https://forward.example.com",
            status="ready",
            parameters={"network_id": "net-1"},
        )
        self.sync = ForwardSync.objects.create(
            name="guard-sync", source=source, auto_merge=True, parameters={}
        )
        self.content_type = ContentType.objects.get_for_model(ForwardSync)

    def _ingestion(self, *, finished, applied=None):
        started = timezone.now() - timedelta(hours=3)
        job = Job.objects.create(
            object_type=self.content_type,
            object_id=self.sync.pk,
            name="guard sync",
            status=JobStatusChoices.STATUS_COMPLETED,
            job_id=uuid4(),
            created=started,
            started=started,
            completed=started + timedelta(minutes=5) if finished else None,
            data={
                "statistics": {
                    model: {"applied": count}
                    for model, count in (applied or {}).items()
                }
            },
        )
        return ForwardIngestion.objects.create(
            sync=self.sync, job=job, change_request_id=uuid4(), snapshot_id="1"
        )

    def _sites(self, count):
        Site.objects.bulk_create(
            [Site(name=f"g{index}", slug=f"g{index}") for index in range(count)]
        )


class HoldTest(GuardTestBase):
    def test_a_big_run_into_an_established_table_is_held(self):
        self._sites(1_200)
        self._ingestion(finished=True)
        current = self._ingestion(finished=False)

        findings = decide_run_size_hold(self.sync, current, {SITE: 30_000})

        self.assertEqual([item["model"] for item in findings], [SITE])
        self.assertEqual(findings[0]["norm"], 1_200)
        self.assertEqual(findings[0]["multiple"], 25.0)

    def test_a_run_far_above_the_typical_run_is_held(self):
        for applied in (300, 250, 280):
            self._ingestion(finished=True, applied={SITE: applied})
        current = self._ingestion(finished=False)

        findings = decide_run_size_hold(self.sync, current, {SITE: 40_000})

        self.assertEqual(findings[0]["basis"], "prior_runs")

    def test_only_the_flagged_model_is_listed(self):
        self._sites(1_200)
        self._ingestion(finished=True)
        current = self._ingestion(finished=False)

        findings = decide_run_size_hold(
            self.sync, current, {SITE: 30_000, "dcim.region": 50}
        )

        self.assertEqual([item["model"] for item in findings], [SITE])


class NeverHeldTest(GuardTestBase):
    def test_the_first_run_a_sync_ever_does_is_never_held(self):
        self._sites(1_200)
        current = self._ingestion(finished=False)

        self.assertEqual(decide_run_size_hold(self.sync, current, {SITE: 90_000}), [])

    def test_an_earlier_run_that_never_finished_is_not_a_prior_run(self):
        self._sites(1_200)
        self._ingestion(finished=False)
        current = self._ingestion(finished=False)

        self.assertEqual(decide_run_size_hold(self.sync, current, {SITE: 90_000}), [])

    def test_a_sync_with_auto_merge_off_is_never_held(self):
        self._sites(1_200)
        self._ingestion(finished=True)
        current = self._ingestion(finished=False)
        self.sync.auto_merge = False
        self.sync.save()

        self.assertEqual(decide_run_size_hold(self.sync, current, {SITE: 90_000}), [])

    def test_a_run_below_the_absolute_floor_is_never_held(self):
        self._sites(1_200)
        self._ingestion(finished=True)
        current = self._ingestion(finished=False)

        self.assertEqual(
            decide_run_size_hold(self.sync, current, {SITE: MIN_CHANGES - 1}), []
        )

    def test_an_empty_or_small_table_has_no_norm_to_be_far_from(self):
        self._sites(999)
        self._ingestion(finished=True)
        current = self._ingestion(finished=False)

        self.assertEqual(decide_run_size_hold(self.sync, current, {SITE: 90_000}), [])

    def test_growth_within_the_multiple_is_never_held(self):
        self._sites(5_000)
        self._ingestion(finished=True)
        current = self._ingestion(finished=False)

        self.assertEqual(decide_run_size_hold(self.sync, current, {SITE: 14_000}), [])

    def test_too_little_history_is_no_norm(self):
        for applied in (300, 250):
            self._ingestion(finished=True, applied={SITE: applied})
        current = self._ingestion(finished=False)

        self.assertEqual(decide_run_size_hold(self.sync, current, {SITE: 40_000}), [])

    def test_no_staged_changes_is_never_held(self):
        self._ingestion(finished=True)
        current = self._ingestion(finished=False)

        self.assertEqual(decide_run_size_hold(self.sync, current, {}), [])


class RecordTest(GuardTestBase):
    def test_the_hold_is_written_through_the_runs_logger(self):
        self._sites(1_200)
        self._ingestion(finished=True)
        current = self._ingestion(finished=False)
        findings = decide_run_size_hold(self.sync, current, {SITE: 30_000})
        logger = RecordingLogger()

        record_hold(logger, findings)

        self.assertEqual(logger.flushed, 1)
        stored = logger.log_data[HOLD_JOB_DATA_KEY]
        self.assertEqual(stored["findings"], findings)
        self.assertIn("dcim.site would stage 30,000 changes", stored["summary"])
        self.assertEqual(stored, hold_record(findings))
