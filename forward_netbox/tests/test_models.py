from unittest.mock import patch

from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import ValidationError
from django.test import TestCase

from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardNQEMap
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.signals import seed_builtin_nqe_maps
from forward_netbox.utilities.forward_api import LATEST_PROCESSED_SNAPSHOT
from forward_netbox.utilities.query_registry import builtin_nqe_map_rows
from forward_netbox.utilities.query_registry import QuerySpec


class ForwardSyncModelTest(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="source-1",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "timeout": 1200,
                "network_id": "235937",
            },
        )

    def test_sync_rejects_query_overrides_parameter(self):
        sync = ForwardSync(
            name="sync-1",
            source=self.source,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
                "query_overrides": {
                    "dcim.device": {
                        "query_id": "FQ_123",
                    }
                },
            },
        )

        with self.assertRaises(ValidationError) as ctx:
            sync.clean()

        self.assertIn("Unsupported Forward sync keys", str(ctx.exception))
        self.assertIn("query_overrides", str(ctx.exception))

    def test_latest_baseline_ingestion_returns_latest_ready_snapshot(self):
        sync = ForwardSync.objects.create(
            name="sync-baseline",
            source=self.source,
            auto_merge=False,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
            },
        )
        ForwardIngestion.objects.create(
            sync=sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="1248263",
            baseline_ready=False,
        )
        expected = ForwardIngestion.objects.create(
            sync=sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="1248264",
            baseline_ready=True,
        )
        ForwardIngestion.objects.create(
            sync=sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="",
            baseline_ready=True,
        )

        self.assertEqual(sync.latest_baseline_ingestion(), expected)

    def test_latest_baseline_ingestion_excludes_current_ingestion(self):
        sync = ForwardSync.objects.create(
            name="sync-baseline-exclude",
            source=self.source,
            auto_merge=False,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
            },
        )
        expected = ForwardIngestion.objects.create(
            sync=sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="1248264",
            baseline_ready=True,
        )
        current = ForwardIngestion.objects.create(
            sync=sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="1248265",
            baseline_ready=True,
        )

        self.assertEqual(
            sync.latest_baseline_ingestion(exclude_ingestion_id=current.pk),
            expected,
        )

    def test_incremental_diff_baseline_requires_latest_processed_and_query_ids(self):
        sync = ForwardSync.objects.create(
            name="sync-diff-baseline",
            source=self.source,
            auto_merge=False,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
            },
        )
        baseline = ForwardIngestion.objects.create(
            sync=sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="1248264",
            baseline_ready=True,
        )
        specs = [
            QuerySpec(
                model_string="dcim.device",
                query_name="Device Query",
                query_id="Q_device",
            )
        ]

        self.assertEqual(
            sync.incremental_diff_baseline(
                specs=specs,
                current_snapshot_id="1248265",
            ),
            baseline,
        )
        self.assertIsNone(
            sync.incremental_diff_baseline(
                specs=[
                    QuerySpec(
                        model_string="dcim.device",
                        query_name="Device Query",
                        query='select {name: "device-1"}',
                    )
                ],
                current_snapshot_id="1248265",
            )
        )
        self.assertIsNone(
            sync.incremental_diff_baseline(
                specs=specs,
                current_snapshot_id="1248264",
            )
        )


class ForwardIngestionSnapshotSummaryTest(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="source-2",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "timeout": 1200,
                "network_id": "235937",
            },
        )
        self.sync = ForwardSync.objects.create(
            name="sync-2",
            source=self.source,
            auto_merge=False,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
            },
        )

    def test_snapshot_summary_helpers_return_expected_fields(self):
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="1248264",
            snapshot_info={
                "state": "PROCESSED",
                "createdAt": "2026-03-31T12:00:00Z",
                "processedAt": "2026-03-31T12:15:00Z",
            },
            snapshot_metrics={
                "snapshotState": "PROCESSED",
                "numSuccessfulDevices": 122,
                "numSuccessfulEndpoints": 1213,
                "processingDuration": 900,
                "extraMetric": "ignored",
            },
        )

        self.assertEqual(
            ingestion.get_snapshot_summary(),
            {
                "snapshot_selector": LATEST_PROCESSED_SNAPSHOT,
                "snapshot_id": "1248264",
                "state": "PROCESSED",
                "created_at": "2026-03-31T12:00:00Z",
                "processed_at": "2026-03-31T12:15:00Z",
            },
        )
        self.assertEqual(
            ingestion.get_snapshot_metrics_summary(),
            {
                "snapshotState": "PROCESSED",
                "numSuccessfulDevices": 122,
                "numSuccessfulEndpoints": 1213,
                "processingDuration": 900,
            },
        )

    def test_ingestion_defaults_to_full_mode_and_not_baseline_ready(self):
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="1248264",
        )

        self.assertEqual(ingestion.sync_mode, "full")
        self.assertFalse(ingestion.baseline_ready)

    def test_sync_merge_can_skip_baseline_marker_for_intermediate_branch(self):
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="1248264",
        )

        with patch("forward_netbox.utilities.merge.merge_branch"):
            ingestion.sync_merge(mark_baseline_ready=False)

        ingestion.refresh_from_db()
        self.assertFalse(ingestion.baseline_ready)


class ForwardNQEMapModelTest(TestCase):
    def test_map_defaults_coalesce_fields_from_model_contract(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="site")
        query_map = ForwardNQEMap(
            name="Site Map",
            netbox_model=netbox_model,
            query='select {\n  name: "site-a",\n  slug: "site-a"\n}',
        )

        query_map.clean()

        self.assertEqual(query_map.coalesce_fields, [["slug"], ["name"]])

    def test_prefix_map_defaults_include_vrf_optional_fallback(self):
        netbox_model = ContentType.objects.get(app_label="ipam", model="prefix")
        query_map = ForwardNQEMap(
            name="Prefix Map",
            netbox_model=netbox_model,
            query='select {\n  prefix: "10.0.0.0/24",\n  vrf: null,\n  status: "active"\n}',
        )

        query_map.clean()

        self.assertEqual(query_map.coalesce_fields, [["prefix", "vrf"], ["prefix"]])

    def test_ipaddress_map_defaults_include_vrf_optional_fallback(self):
        netbox_model = ContentType.objects.get(app_label="ipam", model="ipaddress")
        query_map = ForwardNQEMap(
            name="IP Address Map",
            netbox_model=netbox_model,
            query=(
                'select {\n  device: "device-1",\n  interface: "Ethernet1/1",\n'
                '  address: "10.0.0.1/24",\n  vrf: null,\n  status: "active"\n}'
            ),
        )

        query_map.clean()

        self.assertEqual(query_map.coalesce_fields, [["address", "vrf"], ["address"]])

    def test_map_rejects_invalid_coalesce_field(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="site")
        query_map = ForwardNQEMap(
            name="Site Map",
            netbox_model=netbox_model,
            query='select {\n  name: "site-a",\n  slug: "site-a"\n}',
            coalesce_fields=[["name"], ["invalid_field"]],
        )

        with self.assertRaises(ValidationError) as ctx:
            query_map.clean()

        self.assertIn("is not allowed", str(ctx.exception))

    def test_map_rejects_query_missing_required_fields(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="device")
        query_map = ForwardNQEMap(
            name="Device Map",
            netbox_model=netbox_model,
            query='select {name: "device-1"}',
        )

        with self.assertRaises(ValidationError) as ctx:
            query_map.clean()

        self.assertIn("missing required fields", str(ctx.exception))

    def test_seed_builtin_maps_updates_existing_prefix_map_defaults(self):
        netbox_model = ContentType.objects.get(app_label="ipam", model="prefix")
        query_map = ForwardNQEMap.objects.get(
            name="Forward IPv4 Prefixes",
            netbox_model=netbox_model,
            built_in=True,
        )
        query_map.coalesce_fields = [["prefix", "vrf"]]
        query_map.query = (
            'select {\n  prefix: "10.0.0.0/24",\n  vrf: null,\n  status: "active"\n}'
        )
        query_map.save(update_fields=["coalesce_fields", "query"])

        seed_builtin_nqe_maps(type("Sender", (), {"label": "forward_netbox"}))

        query_map.refresh_from_db()
        expected_row = next(
            row
            for row in builtin_nqe_map_rows()
            if row["model_string"] == "ipam.prefix"
            and row["name"] == "Forward IPv4 Prefixes"
        )
        self.assertEqual(query_map.coalesce_fields, [["prefix", "vrf"], ["prefix"]])
        self.assertEqual(query_map.query, expected_row["query"])

    def test_seed_builtin_maps_updates_existing_inventory_query(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="inventoryitem")
        query_map = ForwardNQEMap.objects.get(
            name="Forward Inventory Items",
            netbox_model=netbox_model,
            built_in=True,
        )
        query_map.query = (
            'select {\n  device: "device-1",\n  name: "fan-1",\n  part_id: "fan-1",\n'
            '  serial: "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz",\n'
            '  status: "active",\n  discovered: true\n}'
        )
        query_map.save(update_fields=["query"])

        seed_builtin_nqe_maps(type("Sender", (), {"label": "forward_netbox"}))

        query_map.refresh_from_db()
        expected_row = next(
            row
            for row in builtin_nqe_map_rows()
            if row["model_string"] == "dcim.inventoryitem"
            and row["name"] == "Forward Inventory Items"
        )
        self.assertEqual(query_map.query, expected_row["query"])
        self.assertIn("truncate(value: String, max_len: Integer)", query_map.query)
