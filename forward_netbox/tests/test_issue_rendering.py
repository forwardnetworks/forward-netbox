from dcim.models import Site
from django.test import TestCase
from rest_framework.test import APIRequestFactory

from forward_netbox.api.serializers import ForwardIngestionIssueSerializer
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardIngestionIssue
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
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
