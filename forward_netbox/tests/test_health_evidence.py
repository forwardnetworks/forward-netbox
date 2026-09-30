"""The evidence a support bundle needs, without anyone running a script.

Row counts beside Forward's, what recent ingestions did to which models, and
whether each merge happened - collected with bounded queries, counts and ids
only. These build real syncs, ingestions, jobs and rows rather than stand-ins.
"""

from datetime import timedelta
from uuid import uuid4

from core.choices import JobStatusChoices
from core.models import Job
from core.models import ObjectChange
from dcim.models import Site
from django.contrib.contenttypes.models import ContentType
from django.test import TestCase
from django.utils import timezone

from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.models import ForwardValidationRun
from forward_netbox.utilities.health import sync_health_summary
from forward_netbox.utilities.health_evidence import applied_change_history
from forward_netbox.utilities.health_evidence import recent_ingestions_evidence
from forward_netbox.utilities.health_evidence import row_count_evidence
from forward_netbox.utilities.run_size_anomaly import HOLD_JOB_DATA_KEY


class EvidenceTestBase(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="ev-source",
            type="saas",
            url="https://forward.example.com",
            status="ready",
            parameters={"network_id": "net-1"},
        )
        self.sync = ForwardSync.objects.create(
            name="ev-sync", source=self.source, parameters={}
        )
        self.content_type = ContentType.objects.get_for_model(ForwardSync)

    def _job(self, *, started, completed, data=None):
        return Job.objects.create(
            object_type=self.content_type,
            object_id=self.sync.pk,
            name="ev sync",
            status=JobStatusChoices.STATUS_COMPLETED,
            job_id=uuid4(),
            created=started,
            started=started,
            completed=completed,
            data=data or {},
        )

    def _ingestion(self, *, started, applied=None, **fields):
        job = self._job(
            started=started,
            completed=started + timedelta(minutes=5),
            data={
                "statistics": {
                    model: {"total": count, "applied": count, "unchanged": 0}
                    for model, count in (applied or {}).items()
                }
            },
        )
        return ForwardIngestion.objects.create(
            sync=self.sync,
            job=job,
            change_request_id=uuid4(),
            snapshot_id="1",
            **fields,
        )


class RowCountEvidenceTest(EvidenceTestBase):
    def _validation_run(self, rows):
        return ForwardValidationRun.objects.create(
            sync=self.sync,
            drift_summary={"models": {"dcim.site": {"row_count": rows}}},
        )

    def test_forward_and_netbox_counts_sit_side_by_side(self):
        self._validation_run(40)
        for index in range(3):
            Site.objects.create(name=f"s{index}", slug=f"s{index}")

        evidence = row_count_evidence(self.sync, ["dcim.site"])

        (entry,) = evidence["models"]
        self.assertEqual(entry["model"], "dcim.site")
        self.assertEqual(entry["forward_rows"], 40)
        self.assertEqual(entry["netbox_rows"], 3)
        self.assertEqual(entry["note"], "")

    def test_rows_are_split_at_the_moment_the_latest_run_started(self):
        for index in range(2):
            Site.objects.create(name=f"old{index}", slug=f"old{index}")
        started = timezone.now() + timedelta(seconds=1)
        self._ingestion(started=started)
        Site.objects.filter(slug__startswith="old").update(
            created=started - timedelta(days=1)
        )
        for index in range(5):
            Site.objects.create(name=f"new{index}", slug=f"new{index}")
        Site.objects.filter(slug__startswith="new").update(
            created=started + timedelta(minutes=1)
        )

        (entry,) = row_count_evidence(self.sync, ["dcim.site"])["models"]

        self.assertEqual(entry["netbox_rows"], 7)
        self.assertEqual(entry["rows_before_latest_run"], 2)
        self.assertEqual(entry["rows_created_since"], 5)

    def test_a_model_that_is_not_installed_says_so(self):
        (entry,) = row_count_evidence(self.sync, ["nothing.installed"])["models"]

        self.assertEqual(entry["note"], "not installed")
        self.assertIsNone(entry["netbox_rows"])

    def test_a_query_that_trips_the_guard_is_reported_not_omitted(self):
        from unittest.mock import patch

        from django.db import OperationalError

        with patch(
            "forward_netbox.utilities.health_evidence._time_limited",
            side_effect=OperationalError("canceling statement due to timeout"),
        ):
            (entry,) = row_count_evidence(self.sync, ["dcim.site"])["models"]

        self.assertIn("skipped", entry["note"])
        self.assertIsNone(entry["netbox_rows"])


class RecentIngestionsEvidenceTest(EvidenceTestBase):
    def test_an_automatic_merge_is_named_as_one_not_as_no_merge(self):
        now = timezone.now()
        ingestion = self._ingestion(
            started=now - timedelta(hours=5),
            applied={"dcim.site": 12},
            sync_mode="full",
            merge_applied_at=now,
            applied_change_count=12,
            created_change_count=12,
        )

        evidence = recent_ingestions_evidence(self.sync)

        (entry,) = evidence["ingestions"]
        self.assertEqual(entry["ingestion"], ingestion.pk)
        self.assertIsNone(entry["merge"]["merge_job_status"])
        self.assertEqual(entry["merge"]["mode"], "automatic_inside_sync_job")
        self.assertEqual(entry["merge"]["counts"]["created"], 12)
        self.assertEqual(entry["statistics_by_model"]["dcim.site"]["applied"], 12)

    def test_an_unmerged_ingestion_is_not_called_merged(self):
        self._ingestion(started=timezone.now())

        (entry,) = recent_ingestions_evidence(self.sync)["ingestions"]

        self.assertEqual(entry["merge"]["mode"], "not_merged")

    def test_only_the_most_recent_three_are_listed_newest_first(self):
        base = timezone.now() - timedelta(days=1)
        created = [
            self._ingestion(started=base + timedelta(hours=index)).pk
            for index in range(5)
        ]

        evidence = recent_ingestions_evidence(self.sync)

        self.assertEqual(
            [item["ingestion"] for item in evidence["ingestions"]],
            list(reversed(created))[:3],
        )

    def test_merged_changes_are_counted_over_every_change_by_model_and_action(self):
        ingestion = self._ingestion(started=timezone.now())
        site_type = ContentType.objects.get_for_model(Site)
        for index in range(7):
            ObjectChange.objects.create(
                request_id=ingestion.change_request_id,
                user_name="forward",
                changed_object_type=site_type,
                changed_object_id=index + 1,
                object_repr=f"s{index}",
                action="create",
            )
        ObjectChange.objects.create(
            request_id=ingestion.change_request_id,
            user_name="forward",
            changed_object_type=site_type,
            changed_object_id=1,
            object_repr="s0",
            action="update",
        )
        ObjectChange.objects.create(
            request_id=uuid4(),
            user_name="forward",
            changed_object_type=site_type,
            changed_object_id=99,
            object_repr="other",
            action="create",
        )

        (entry,) = recent_ingestions_evidence(self.sync)["ingestions"]

        merged = entry["merged_changes_by_model_action"]
        self.assertEqual(merged["total"], 8)
        self.assertEqual(
            merged["counts"], {"dcim.site.create": 7, "dcim.site.update": 1}
        )

    def test_auto_merge_setting_and_status_travel_with_it(self):
        self.sync.auto_merge = True
        self.sync.save()

        evidence = recent_ingestions_evidence(self.sync)

        self.assertTrue(evidence["auto_merge"])
        self.assertEqual(evidence["sync"], self.sync.pk)

    def test_no_names_or_config_text_are_carried(self):
        self._ingestion(started=timezone.now(), applied={"dcim.site": 1})

        rendered = repr(recent_ingestions_evidence(self.sync))

        self.assertNotIn("ev-sync", rendered)
        self.assertNotIn("ev-source", rendered)


class AppliedHistoryTest(EvidenceTestBase):
    def test_history_skips_the_latest_and_drops_empty_runs(self):
        base = timezone.now() - timedelta(days=2)
        self._ingestion(started=base, applied={"dcim.site": 100})
        self._ingestion(started=base + timedelta(hours=1), applied={"dcim.site": 0})
        self._ingestion(started=base + timedelta(hours=2), applied={"dcim.site": 300})
        self._ingestion(started=base + timedelta(hours=3), applied={"dcim.site": 9999})

        history = applied_change_history(self.sync)

        self.assertEqual(sorted(history["dcim.site"]), [100, 300])


class HealthWiringTest(EvidenceTestBase):
    def test_the_sync_page_summary_omits_evidence_and_the_health_tab_has_it(self):
        self._ingestion(started=timezone.now())

        light = sync_health_summary(self.sync)
        full = sync_health_summary(self.sync, include_evidence=True)

        self.assertFalse(light["evidence_included"])
        self.assertEqual(light["row_counts"], {})
        self.assertEqual(light["recent_ingestions"], {})
        self.assertTrue(full["evidence_included"])
        self.assertIn("models", full["row_counts"])
        self.assertEqual(len(full["recent_ingestions"]["ingestions"]), 1)

    def test_netbox_far_below_forward_is_a_check_with_no_url(self):
        self._ingestion(started=timezone.now())
        ForwardValidationRun.objects.create(
            sync=self.sync,
            drift_summary={"models": {"dcim.site": {"row_count": 5000}}},
        )
        Site.objects.create(name="only", slug="only")

        checks = sync_health_summary(self.sync, include_evidence=True)["checks"]

        gap = [c for c in checks if c["name"].startswith("NetBox holds far fewer")]
        self.assertEqual(len(gap), 1)
        self.assertEqual(gap[0]["status"], "warn")
        self.assertIn("dcim.site: NetBox holds 1 of the 5,000", gap[0]["message"])
        self.assertNotIn("url", gap[0])

    def test_no_gap_check_before_the_first_ingestion(self):
        ForwardValidationRun.objects.create(
            sync=self.sync,
            drift_summary={"models": {"dcim.site": {"row_count": 5000}}},
        )

        checks = sync_health_summary(self.sync, include_evidence=True)["checks"]

        self.assertFalse(
            [c for c in checks if c["name"].startswith("NetBox holds far fewer")]
        )


class RunSizeCheckTest(EvidenceTestBase):
    def test_a_run_many_times_the_typical_run_is_flagged_with_a_link(self):
        base = timezone.now() - timedelta(days=5)
        for index, applied in enumerate((150, 200, 180)):
            self._ingestion(
                started=base + timedelta(hours=index),
                applied={"dcim.site": applied},
            )
        latest = self._ingestion(
            started=base + timedelta(hours=9), applied={"dcim.site": 120_000}
        )

        checks = sync_health_summary(self.sync)["checks"]

        (check,) = [c for c in checks if c["name"] == "Unusually large run"]
        self.assertEqual(check["status"], "warn")
        self.assertIn("dcim.site staged 120,000 changes", check["message"])
        self.assertEqual(check["url"], latest.get_absolute_url())

    def test_a_first_load_with_no_history_is_not_flagged(self):
        self._ingestion(started=timezone.now(), applied={"dcim.site": 500_000})

        checks = sync_health_summary(self.sync)["checks"]

        self.assertFalse([c for c in checks if c["name"] == "Unusually large run"])

    def test_a_recorded_hold_is_named_while_the_ingestion_can_still_merge(self):
        from unittest.mock import patch

        ingestion = self._ingestion(started=timezone.now())
        ingestion.job.data = {
            HOLD_JOB_DATA_KEY: {
                "findings": [
                    {
                        "model": "netbox_routing.bgppeer",
                        "changes": 127_422,
                        "multiple": 7.7,
                        "basis": "netbox_rows",
                        "norm": 16_605,
                        "runs": None,
                    }
                ]
            }
        }
        ingestion.job.save()

        with patch.object(ForwardIngestion, "can_queue_merge", new=True):
            checks = sync_health_summary(self.sync)["checks"]

        (check,) = [c for c in checks if c["name"] == "Run held for review"]
        self.assertIn("7.7x the 16,605 rows NetBox already holds", check["message"])
        self.assertIn("delete the ingestion to discard", check["message"])
        self.assertEqual(check["url"], ingestion.get_absolute_url())

    def test_a_hold_on_an_ingestion_that_can_no_longer_merge_is_not_shown(self):
        ingestion = self._ingestion(started=timezone.now())
        ingestion.job.data = {HOLD_JOB_DATA_KEY: {"findings": []}}
        ingestion.job.save()

        checks = sync_health_summary(self.sync)["checks"]

        self.assertFalse([c for c in checks if c["name"] == "Run held for review"])


class BaselineNeverCompletedTest(EvidenceTestBase):
    def test_finished_runs_and_no_baseline_is_flagged(self):
        base = timezone.now() - timedelta(days=2)
        self._ingestion(started=base)
        self._ingestion(started=base + timedelta(hours=1))

        checks = sync_health_summary(self.sync)["checks"]

        (check,) = [c for c in checks if c["name"] == "Baseline never completed"]
        self.assertIn("2 runs have finished", check["message"])

    def test_a_promoted_baseline_clears_it(self):
        base = timezone.now() - timedelta(days=2)
        self._ingestion(started=base, baseline_ready=True)
        self._ingestion(started=base + timedelta(hours=1))

        checks = sync_health_summary(self.sync)["checks"]

        self.assertFalse([c for c in checks if c["name"] == "Baseline never completed"])

    def test_a_single_run_is_not_yet_a_pattern(self):
        self._ingestion(started=timezone.now())

        checks = sync_health_summary(self.sync)["checks"]

        self.assertFalse([c for c in checks if c["name"] == "Baseline never completed"])
