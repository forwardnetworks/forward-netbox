from dcim.models import Site
from django.test import TestCase
from rest_framework.test import APIRequestFactory

from forward_netbox.api.serializers import ForwardIngestionIssueSerializer
from forward_netbox.api.serializers import ForwardIngestionSerializer
from forward_netbox.choices import ForwardValidationStatusChoices
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardIngestionIssue
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.models import ForwardValidationRun
from forward_netbox.tables import ForwardIngestionIssueTable


class ForwardIngestionIssueRenderingTest(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="source-rendering",
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
            name="sync-rendering",
            source=self.source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        self.ingestion = ForwardIngestion.objects.create(sync=self.sync)

    def test_issue_table_json_render_sanitizes_model_values(self):
        site = Site.objects.create(name="site-1", slug="site-1")
        table = ForwardIngestionIssueTable([])

        rendered = table._render_json({"site": site})

        self.assertIn("dcim.site", rendered)
        self.assertIn(str(site.pk), rendered)

    def test_issue_serializer_sanitizes_model_values(self):
        site = Site.objects.create(name="site-1", slug="site-1")
        issue = ForwardIngestionIssue(
            ingestion=self.ingestion,
            phase="sync",
            model="netbox_routing.bgppeer",
            message="routing failed",
            coalesce_fields={"site": site},
            defaults={"router": site},
            raw_data={"site": site},
            exception="ForwardSyncDataError",
        )

        request = APIRequestFactory().get("/")
        data = ForwardIngestionIssueSerializer(issue, context={"request": request}).data

        self.assertEqual(data["coalesce_fields"]["site"]["model"], "dcim.site")
        self.assertEqual(data["defaults"]["router"]["pk"], site.pk)
        self.assertEqual(data["raw_data"]["site"]["display"], str(site))

    def test_ingestion_serializer_exposes_analysis_summary(self):
        validation_run = ForwardValidationRun.objects.create(
            sync=self.sync,
            status=ForwardValidationStatusChoices.BLOCKED,
            allowed=False,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-before",
            baseline_snapshot_id="snapshot-baseline",
            blocking_reasons=["blocked"],
            drift_summary={"model_count": 1},
        )
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            validation_run=validation_run,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-before",
            baseline_ready=True,
            sync_mode="diff",
        )
        ForwardIngestionIssue.objects.create(
            ingestion=ingestion,
            phase="sync",
            model="dcim.device",
            message="device warning",
            exception="warning",
        )

        request = APIRequestFactory().get("/")
        data = ForwardIngestionSerializer(ingestion, context={"request": request}).data

        self.assertEqual(data["analysis_summary"]["baseline_ready"], True)
        self.assertEqual(data["analysis_summary"]["issue_count"], 1)
        self.assertEqual(data["analysis_summary"]["validation_run"], validation_run.pk)
        self.assertEqual(data["analysis_summary"]["validation_status"], "blocked")
        self.assertEqual(
            data["sync"]["analysis_summary"]["latest_validation_run"], validation_run.pk
        )
        self.assertEqual(data["sync"]["analysis_summary"]["baseline_ready"], True)
        self.assertEqual(data["workload_summary"]["sync_mode"], "diff")
        self.assertEqual(data["sync"]["workload_summary"]["uses_multi_branch"], True)
        self.assertEqual(data["advisory_summary"]["intent_signals"]["issue_count"], 1)
        self.assertEqual(
            data["sync"]["advisory_summary"]["latest_validation_run"], validation_run.pk
        )
