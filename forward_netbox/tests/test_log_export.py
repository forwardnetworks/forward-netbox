import json

from core.choices import JobStatusChoices
from core.models import Job
from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync


class ForwardIngestionLogExportViewTest(TestCase):
    @classmethod
    def setUpTestData(cls):
        User = get_user_model()
        cls.user = User.objects.create_superuser(
            username="log-export-admin",
            password="TestPassword123!",
            email="admin@example.com",
        )
        cls.source = ForwardSource.objects.create(
            name="source-log-export",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "network_id": "test-network",
            },
        )
        cls.sync = ForwardSync.objects.create(
            name="sync-log-export",
            source=cls.source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        cls.ingestion = ForwardIngestion.objects.create(
            sync=cls.sync,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-1",
        )

        now = timezone.now()
        cls.job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardIngestion),
            object_id=cls.ingestion.pk,
            name="ingestion-log-export-job",
            user=None,
            status=JobStatusChoices.STATUS_COMPLETED,
            job_id="123e4567-e89b-12d3-a456-426614174000",
            created=now,
            started=now,
            completed=now,
            data={
                "logs": [
                    [
                        now.isoformat(),
                        "success",
                        "sync-log-export",
                        "/plugins/forward/ingestion/1/",
                        "Synthetic sync stage completed.",
                    ]
                ],
                "statistics": {"dcim.site": {"current": 1, "total": 1}},
            },
        )
        cls.merge_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardIngestion),
            object_id=cls.ingestion.pk,
            name="ingestion-log-export-merge-job",
            user=None,
            status=JobStatusChoices.STATUS_COMPLETED,
            job_id="223e4567-e89b-12d3-a456-426614174001",
            created=now,
            started=now,
            completed=now,
            data={
                "logs": [
                    [
                        now.isoformat(),
                        "failure",
                        "sync-log-export",
                        "/plugins/forward/ingestion/1/",
                        "Synthetic merge stage failed.",
                    ]
                ],
                "statistics": {"dcim.site": {"current": 0, "total": 1}},
            },
        )
        cls.job.log_entries = [
            {
                "timestamp": now,
                "level": "info",
                "message": "Synthetic sync stage completed.",
            }
        ]
        cls.job.save(update_fields=["log_entries"])
        cls.merge_job.log_entries = [
            {
                "timestamp": now,
                "level": "error",
                "message": "Synthetic merge stage failed.",
            }
        ]
        cls.merge_job.save(update_fields=["log_entries"])
        cls.ingestion.job = cls.job
        cls.ingestion.merge_job = cls.merge_job
        cls.ingestion.save(update_fields=["job", "merge_job"])

    def test_export_logs_downloads_json_bundle(self):
        self.client.force_login(self.user)

        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardingestion_export_logs",
                kwargs={"pk": self.ingestion.pk},
            )
            + "?stage=merge"
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn("attachment;", response["Content-Disposition"])
        self.assertIn("forward-ingestion-", response["Content-Disposition"])

        data = json.loads(response.content)
        self.assertEqual(data["active_stage"], "merge")
        self.assertEqual(data["ingestion"]["pk"], self.ingestion.pk)
        self.assertEqual(data["ingestion"]["job"]["pk"], self.job.pk)
        self.assertEqual(data["ingestion"]["merge_job"]["pk"], self.merge_job.pk)
        self.assertEqual(
            data["job_results"]["logs"][0][4], "Synthetic sync stage completed."
        )
        self.assertEqual(
            data["merge_job_results"]["logs"][0][4],
            "Synthetic merge stage failed.",
        )
        self.assertEqual(data["job_results"]["statistics"]["dcim.site"]["total"], 1)
        self.assertEqual(
            data["merge_job_results"]["statistics"]["dcim.site"]["total"], 1
        )
