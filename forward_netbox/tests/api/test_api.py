from django.contrib.contenttypes.models import ContentType
from django.utils import timezone
from rest_framework import status
from utilities.testing import APIViewTestCases

from forward_netbox.models import ForwardData
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardIngestionIssue
from forward_netbox.models import ForwardNQEQuery
from forward_netbox.models import ForwardSnapshot
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync


BASE = "/api/plugins/forward/"


class ForwardNQEQueryTest(APIViewTestCases.APIViewTestCase):
    model = ForwardNQEQuery
    brief_fields = [
        "display",
        "id",
        "content_type",
        "query_id",
        "enabled",
    ]
    bulk_update_data = {
        "enabled": False,
    }

    def _get_list_url(self):
        return f"{BASE}nqe-map/"

    def _get_detail_url(self, instance):
        return f"{BASE}nqe-map/{instance.pk}/"

    @classmethod
    def setUpTestData(cls):
        device_ct = ContentType.objects.get(app_label="dcim", model="device")
        interface_ct = ContentType.objects.get(app_label="dcim", model="interface")

        ForwardNQEQuery.objects.update_or_create(
            content_type=device_ct,
            defaults={"query_id": "FQ_default_device", "enabled": True},
        )
        ForwardNQEQuery.objects.update_or_create(
            content_type=interface_ct,
            defaults={"query_id": "FQ_default_interface", "enabled": True},
        )

        cls.create_data = [
            {
                "content_type": "dcim.location",
                "query_id": "FQ_new_location",
                "enabled": True,
            },
            {
                "content_type": "dcim.devicerole",
                "query_id": "FQ_new_role",
                "enabled": False,
            },
        ]

class ForwardSourceTest(APIViewTestCases.APIViewTestCase):
    model = ForwardSource
    brief_fields = [
        "display",
        "id",
        "name",
        "status",
        "type",
        "url",
    ]
    bulk_update_data = {
        "url": "https://updated.local",
    }
    graphql_base_name = "forward_source"

    def _get_list_url(self):
        return f"{BASE}source/"

    def _get_detail_url(self, instance):
        return f"{BASE}source/{instance.pk}/"

    @classmethod
    def setUpTestData(cls):
        ForwardSource.objects.create(
            name="Source A",
            url="https://a.local",
            parameters={"auth": "t", "verify": True},
            last_synced=timezone.now(),
        )
        ForwardSource.objects.create(
            name="Source B",
            url="https://b.local",
            parameters={"auth": "t", "verify": False},
            last_synced=timezone.now(),
        )
        ForwardSource.objects.create(
            name="Source C",
            url="https://c.local",
            parameters={"auth": "t", "verify": False},
            last_synced=timezone.now(),
        )

        cls.create_data = [
            {
                "name": "NewSrc 1",
                "url": "https://nb1.example",
                "parameters": {"auth": "t", "verify": False},
                "type": "local",
            },
            {
                "name": "NewSrc 2",
                "url": "https://nb2.example",
                "parameters": {"auth": "t", "verify": True},
                "type": "local",
            },
            {
                "name": "NewSrc 3",
                "url": "https://nb3.example",
                "parameters": {"auth": "t", "verify": True},
                "type": "local",
            },
        ]


class ForwardSnapshotTest(
    APIViewTestCases.GetObjectViewTestCase,
    APIViewTestCases.ListObjectsViewTestCase,
    APIViewTestCases.GraphQLTestCase,
):
    model = ForwardSnapshot
    graphql_base_name = "forward_snapshot"
    brief_fields = [
        "data",
        "date",
        "display",
        "id",
        "name",
        "snapshot_id",
        "source",
        "status",
    ]

    def _get_list_url(self):
        return f"{BASE}snapshot/"

    def _get_detail_url(self, instance):
        return f"{BASE}snapshot/{instance.pk}/"

    @classmethod
    def setUpTestData(cls):
        sources = (
            ForwardSource.objects.create(
                name="Source A",
                url="https://src.local",
                parameters={"auth": "t", "verify": True},
                last_synced=timezone.now(),
            ),
            ForwardSource.objects.create(
                name="Source B",
                url="https://srcb.local",
                parameters={"auth": "t", "verify": True},
                last_synced=timezone.now(),
            ),
            ForwardSource.objects.create(
                name="Source C",
                url="https://srcc.local",
                parameters={"auth": "t", "verify": True},
                last_synced=timezone.now(),
            ),
        )

        cls.snapshots = (
            ForwardSnapshot.objects.create(
                name="Snapshot One",
                source=sources[0],
                snapshot_id="snap-1",
                status="loaded",
                data={"sites": ["SiteA", "SiteB", "RemoteC"]},
                date=timezone.now(),
                last_updated=timezone.now(),
            ),
            ForwardSnapshot.objects.create(
                name="Another Name",
                source=sources[0],
                snapshot_id="snap-2",
                status="loaded",
                data={"sites": []},
                date=timezone.now(),
                last_updated=timezone.now(),
            ),
            ForwardSnapshot.objects.create(
                name="Third Snapshot",
                source=sources[0],
                snapshot_id="snap-3",
                status="unloaded",
                data={"sites": ["SiteD"]},
                date=timezone.now(),
                last_updated=timezone.now(),
            ),
        )

    def test_sites_action_lists_all_and_filters(self):
        self.add_permissions("forward_netbox.view_forwardsnapshot")
        # list all
        url = f"{BASE}snapshot/{self.snapshots[0].pk}/sites/"
        resp = self.client.get(url, **self.header)
        self.assertHttpStatus(resp, status.HTTP_200_OK)
        body = resp.json()
        self.assertIn(body.__class__, (list, dict))
        if isinstance(body, dict):
            labels = [i["name"] for i in body["results"]]
            self.assertEqual(labels, self.snapshots[0].data["sites"])
        # filter
        url = f"{BASE}snapshot/{self.snapshots[0].pk}/sites/?q=site"
        resp = self.client.get(url, **self.header)
        self.assertHttpStatus(resp, status.HTTP_200_OK)
        body = resp.json()
        if isinstance(body, dict) and body.get("results"):
            labels = [i["name"].lower() for i in body["results"]]
            self.assertTrue(all("site" in name for name in labels))

    def test_raw_patch_and_delete(self):
        self.add_permissions(
            "forward_netbox.view_forwardsnapshot",
            "forward_netbox.change_forwardsnapshot",
            "forward_netbox.delete_forwardsnapshot",
        )
        # initial count
        self.assertEqual(
            ForwardData.objects.filter(snapshot_data=self.snapshots[0]).count(), 0
        )
        # PATCH raw
        url = f"{BASE}snapshot/{self.snapshots[0].pk}/raw/"
        payload = {
            "data": [
                {"data": {"example": 1}, "type": "device"},
                {"data": {"foo": "bar"}, "type": "interface"},
            ]
        }
        resp = self.client.patch(url, data=payload, format="json", **self.header)
        self.assertHttpStatus(resp, status.HTTP_200_OK)
        self.assertEqual(resp.data, {"status": "success"})
        self.assertEqual(
            ForwardData.objects.filter(snapshot_data=self.snapshots[0]).count(), 2
        )
        # DELETE raw
        resp = self.client.delete(url, **self.header)
        self.assertHttpStatus(resp, status.HTTP_200_OK)
        self.assertEqual(resp.data, {"status": "success"})
        self.assertEqual(
            ForwardData.objects.filter(snapshot_data=self.snapshots[0]).count(), 0
        )


class ForwardSyncTest(APIViewTestCases.APIViewTestCase):
    model = ForwardSync
    graphql_base_name = "forward_sync"
    brief_fields = [
        "auto_merge",
        "id",
        "last_synced",
        "name",
        "parameters",
        "status",
    ]
    create_data = [
        {
            "name": "Test Sync A",
            "parameters": {"site": True, "device": False},
        },
        {
            "name": "Test Sync B",
            "parameters": {"ipaddress": True, "prefix": True},
            "auto_merge": True,
        },
        {
            "name": "Test Sync C",
            "parameters": {"device": True, "interface": True},
            "interval": 60,
        },
    ]
    bulk_update_data = {
        "auto_merge": True,
    }

    def _get_list_url(self):
        return f"{BASE}sync/"

    def _get_detail_url(self, instance):
        return f"{BASE}sync/{instance.pk}/"

    @classmethod
    def setUpTestData(cls):
        # Create sources for the snapshots
        sources = (
            ForwardSource.objects.create(
                name="Sync Test Source A",
                url="https://sync-a.local",
                parameters={"auth": "token", "verify": True},
                last_synced=timezone.now(),
            ),
            ForwardSource.objects.create(
                name="Sync Test Source B",
                url="https://sync-b.local",
                parameters={"auth": "token", "verify": False},
                last_synced=timezone.now(),
            ),
            ForwardSource.objects.create(
                name="Sync Test Source C",
                url="https://sync-c.local",
                parameters={"auth": "token", "verify": True},
                last_synced=timezone.now(),
            ),
        )

        # Create snapshots for the syncs
        snapshots = (
            ForwardSnapshot.objects.create(
                name="Sync Test Snapshot A",
                source=sources[0],
                snapshot_id="sync-snap-a",
                status="loaded",
                data={"sites": ["SyncSiteA", "SyncSiteB"]},
                date=timezone.now(),
                last_updated=timezone.now(),
            ),
            ForwardSnapshot.objects.create(
                name="Sync Test Snapshot B",
                source=sources[1],
                snapshot_id="sync-snap-b",
                status="loaded",
                data={"devices": ["SyncDevice1", "SyncDevice2"]},
                date=timezone.now(),
                last_updated=timezone.now(),
            ),
            ForwardSnapshot.objects.create(
                name="Sync Test Snapshot C",
                source=sources[2],
                snapshot_id="sync-snap-c",
                status="unloaded",
                data={"interfaces": ["SyncInterface1"]},
                date=timezone.now(),
                last_updated=timezone.now(),
            ),
        )

        # Create syncs for testing
        ForwardSync.objects.create(
            name="Sync Test D",
            snapshot_data=snapshots[0],
            parameters={"site": True, "device": False},
        )
        ForwardSync.objects.create(
            name="Sync Test E",
            snapshot_data=snapshots[1],
            parameters={"device": True, "interface": True},
            auto_merge=False,
        )
        ForwardSync.objects.create(
            name="Sync Test F",
            snapshot_data=snapshots[2],
            parameters={"ipaddress": True, "prefix": False},
            interval=30,
        )

        # Update create_data to reference the snapshots
        cls.create_data[0]["snapshot_data"] = snapshots[0].pk
        cls.create_data[1]["snapshot_data"] = snapshots[1].pk
        cls.create_data[2]["snapshot_data"] = snapshots[2].pk
        cls.create_data[0]["parameters"] = {"site": True, "device": False}
        cls.create_data[1]["parameters"] = {"ipaddress": True, "prefix": True}
        cls.create_data[2]["parameters"] = {"device": True, "interface": True}


class ForwardIngestionTest(
    APIViewTestCases.GetObjectViewTestCase,
    APIViewTestCases.ListObjectsViewTestCase,
    APIViewTestCases.GraphQLTestCase,
):
    model = ForwardIngestion
    graphql_base_name = "forward_ingestion"
    brief_fields = [
        "branch",
        "id",
        "name",
        "sync",
    ]

    def _get_list_url(self):
        return f"{BASE}ingestion/"

    def _get_detail_url(self, instance):
        return f"{BASE}ingestion/{instance.pk}/"

    @classmethod
    def setUpTestData(cls):
        # Create sources for the snapshots
        sources = (
            ForwardSource.objects.create(
                name="Ingestion Test Source A",
                url="https://ingestion-a.local",
                parameters={"auth": "token", "verify": True},
                last_synced=timezone.now(),
            ),
            ForwardSource.objects.create(
                name="Ingestion Test Source B",
                url="https://ingestion-b.local",
                parameters={"auth": "token", "verify": False},
                last_synced=timezone.now(),
            ),
            ForwardSource.objects.create(
                name="Ingestion Test Source C",
                url="https://ingestion-c.local",
                parameters={"auth": "token", "verify": True},
                last_synced=timezone.now(),
            ),
        )

        # Create snapshots for the syncs
        snapshots = (
            ForwardSnapshot.objects.create(
                name="Ingestion Test Snapshot A",
                source=sources[0],
                snapshot_id="ing-snap-a",
                status="loaded",
                data={"sites": ["SiteA", "SiteB"]},
                date=timezone.now(),
                last_updated=timezone.now(),
            ),
            ForwardSnapshot.objects.create(
                name="Ingestion Test Snapshot B",
                source=sources[1],
                snapshot_id="ing-snap-b",
                status="loaded",
                data={"devices": ["Device1", "Device2"]},
                date=timezone.now(),
                last_updated=timezone.now(),
            ),
            ForwardSnapshot.objects.create(
                name="Ingestion Test Snapshot C",
                source=sources[2],
                snapshot_id="ing-snap-c",
                status="unloaded",
                data={"interfaces": ["Interface1"]},
                date=timezone.now(),
                last_updated=timezone.now(),
            ),
        )

        # Create syncs for the ingestions
        syncs = (
            ForwardSync.objects.create(
                name="Ingestion Test Sync A",
                snapshot_data=snapshots[0],
                parameters={"site": True, "device": False},
            ),
            ForwardSync.objects.create(
                name="Ingestion Test Sync B",
                snapshot_data=snapshots[1],
                parameters={"device": True, "interface": True},
            ),
            ForwardSync.objects.create(
                name="Ingestion Test Sync C",
                snapshot_data=snapshots[2],
                parameters={"ipaddress": True, "prefix": False},
            ),
        )

        # Create ingestions for testing
        ForwardIngestion.objects.create(sync=syncs[0])
        ForwardIngestion.objects.create(sync=syncs[1])
        ForwardIngestion.objects.create(sync=syncs[2])


class ForwardIngestionIssueTest(
    APIViewTestCases.GetObjectViewTestCase,
    APIViewTestCases.ListObjectsViewTestCase,
    APIViewTestCases.GraphQLTestCase,
):
    model = ForwardIngestionIssue
    graphql_base_name = "forward_ingestion_issue"
    brief_fields = [
        "exception",
        "id",
        "ingestion",
        "message",
        "model",
    ]

    def _get_list_url(self):
        return f"{BASE}ingestion-issues/"

    def _get_detail_url(self, instance):
        return f"{BASE}ingestion-issues/{instance.pk}/"

    @classmethod
    def setUpTestData(cls):
        # Create sources for the snapshots
        sources = (
            ForwardSource.objects.create(
                name="Issue Test Source A",
                url="https://issue-a.local",
                parameters={"auth": "token", "verify": True},
                last_synced=timezone.now(),
            ),
            ForwardSource.objects.create(
                name="Issue Test Source B",
                url="https://issue-b.local",
                parameters={"auth": "token", "verify": False},
                last_synced=timezone.now(),
            ),
            ForwardSource.objects.create(
                name="Issue Test Source C",
                url="https://issue-c.local",
                parameters={"auth": "token", "verify": True},
                last_synced=timezone.now(),
            ),
        )

        # Create snapshots for the syncs
        snapshots = (
            ForwardSnapshot.objects.create(
                name="Issue Test Snapshot A",
                source=sources[0],
                snapshot_id="issue-snap-a",
                status="loaded",
                data={"sites": ["IssueTestSiteA", "IssueTestSiteB"]},
                date=timezone.now(),
                last_updated=timezone.now(),
            ),
            ForwardSnapshot.objects.create(
                name="Issue Test Snapshot B",
                source=sources[1],
                snapshot_id="issue-snap-b",
                status="loaded",
                data={"devices": ["IssueTestDevice1", "IssueTestDevice2"]},
                date=timezone.now(),
                last_updated=timezone.now(),
            ),
            ForwardSnapshot.objects.create(
                name="Issue Test Snapshot C",
                source=sources[2],
                snapshot_id="issue-snap-c",
                status="unloaded",
                data={"interfaces": ["IssueTestInterface1"]},
                date=timezone.now(),
                last_updated=timezone.now(),
            ),
        )

        # Create syncs for the ingestions
        syncs = (
            ForwardSync.objects.create(
                name="Issue Test Sync A",
                snapshot_data=snapshots[0],
                parameters={"site": True, "device": False},
            ),
            ForwardSync.objects.create(
                name="Issue Test Sync B",
                snapshot_data=snapshots[1],
                parameters={"device": True, "interface": True},
            ),
            ForwardSync.objects.create(
                name="Issue Test Sync C",
                snapshot_data=snapshots[2],
                parameters={"ipaddress": True, "prefix": False},
            ),
        )

        # Create ingestions for the issues
        ingestions = (
            ForwardIngestion.objects.create(sync=syncs[0]),
            ForwardIngestion.objects.create(sync=syncs[1]),
            ForwardIngestion.objects.create(sync=syncs[2]),
        )

        # Create ingestion issues for testing
        ForwardIngestionIssue.objects.create(
            ingestion=ingestions[0],
            model="dcim.site",
            message="Failed to create site due to validation error",
            raw_data='{"name": "Invalid Site", "slug": ""}',
            coalesce_fields="name,slug",
            defaults="{}",
            exception="ValidationError: Slug field cannot be empty",
        )
        ForwardIngestionIssue.objects.create(
            ingestion=ingestions[0],
            model="dcim.device",
            message="Device type not found",
            raw_data='{"hostname": "test-device", "device_type": "NonExistentType"}',
            coalesce_fields="hostname",
            defaults='{"status": "active"}',
            exception="DoesNotExist: DeviceType matching query does not exist",
        )
        ForwardIngestionIssue.objects.create(
            ingestion=ingestions[1],
            model="dcim.interface",
            message="Interface creation failed - invalid MAC address",
            raw_data='{"name": "eth0", "mac_address": "invalid-mac", "device": 1}',
            coalesce_fields="name,device",
            defaults='{"type": "1000base-t"}',
            exception="ValidationError: Enter a valid MAC address",
        )
        ForwardIngestionIssue.objects.create(
            ingestion=ingestions[2],
            model="ipam.ipaddress",
            message="IP address already exists",
            raw_data='{"address": "192.168.1.1/24", "status": "active"}',
            coalesce_fields="address",
            defaults='{"dns_name": ""}',
            exception="IntegrityError: IP address 192.168.1.1/24 already exists",
        )
