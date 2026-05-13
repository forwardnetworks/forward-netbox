from datetime import datetime
from types import SimpleNamespace

from dcim.models import Site
from django.test import TestCase
from rq.timeouts import JobTimeoutException

from forward_netbox.choices import ForwardIngestionPhaseChoices
from forward_netbox.jobs import record_timeout_issue
from forward_netbox.jobs import safe_save_job_data
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardIngestionIssue
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync


class ForwardJobsTest(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="source-jobs",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "timeout": 1200,
                "network_id": "test-network",
            },
        )
        self.sync = ForwardSync.objects.create(
            name="sync-jobs",
            source=self.source,
            auto_merge=False,
            parameters={"snapshot_id": "latestProcessed"},
        )
        self.ingestion = ForwardIngestion.objects.create(sync=self.sync)

    def test_record_timeout_issue_creates_single_issue_per_ingestion_phase(self):
        issue_1 = record_timeout_issue(
            self.ingestion,
            ForwardIngestionPhaseChoices.SYNC,
            "timeout",
        )
        issue_2 = record_timeout_issue(
            self.ingestion,
            ForwardIngestionPhaseChoices.SYNC,
            "timeout again",
        )

        self.assertEqual(issue_1.pk, issue_2.pk)
        self.assertEqual(
            ForwardIngestionIssue.objects.filter(
                ingestion=self.ingestion,
                phase=ForwardIngestionPhaseChoices.SYNC,
                exception=JobTimeoutException.__name__,
            ).count(),
            1,
        )

    def test_safe_save_job_data_persists_job_log_entries(self):
        class DummyJob:
            pk = 52

            def __init__(self):
                self.data = None
                self.log_entries = []
                self.saved_update_fields = None

            def save(self, update_fields=None):
                self.saved_update_fields = update_fields

        job = DummyJob()
        obj_with_logger = SimpleNamespace(
            logger=SimpleNamespace(
                log_data={
                    "logs": [
                        [
                            "2026-05-03T14:34:00+00:00",
                            "success",
                            "ui-harness-sync",
                            "/plugins/forward/sync/2/",
                            "Synthetic UI harness ingestion completed.",
                        ]
                    ],
                    "statistics": {},
                }
            )
        )

        safe_save_job_data(job, obj_with_logger)

        self.assertEqual(
            job.data["logs"][0][4],
            "Synthetic UI harness ingestion completed.",
        )
        self.assertEqual(len(job.log_entries), 1)
        self.assertEqual(job.log_entries[0]["level"], "info")
        self.assertEqual(
            job.log_entries[0]["message"],
            "Synthetic UI harness ingestion completed.",
        )
        self.assertEqual(job.saved_update_fields, ["data", "log_entries"])

    def test_safe_save_job_data_serializes_nested_model_values(self):
        class DummyJob:
            pk = 53

            def __init__(self):
                self.data = None
                self.log_entries = []
                self.saved_update_fields = None

            def save(self, update_fields=None):
                self.saved_update_fields = update_fields

        site = Site.objects.create(name="site-1", slug="site-1")
        job = DummyJob()
        obj_with_logger = SimpleNamespace(
            logger=SimpleNamespace(
                log_data={
                    "logs": [
                        [
                            datetime.fromisoformat(
                                "2026-05-04T14:00:00+00:00"
                            ).isoformat(),
                            "success",
                            site,
                            "/plugins/forward/sync/2/",
                            "Synthetic UI harness ingestion completed.",
                        ]
                    ],
                    "statistics": {"dcim.site": {"last_object": site}},
                }
            )
        )

        safe_save_job_data(job, obj_with_logger)

        self.assertEqual(job.data["logs"][0][2]["model"], "dcim.site")
        self.assertEqual(
            job.data["statistics"]["dcim.site"]["last_object"]["pk"], site.pk
        )
        self.assertEqual(job.saved_update_fields, ["data", "log_entries"])
