import json
from unittest.mock import patch

from core.choices import JobStatusChoices
from core.choices import ObjectChangeActionChoices
from core.models import Job
from core.models import ObjectType
from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone
from netbox_branching.models import Branch
from netbox_branching.models import ChangeDiff

from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync


class ForwardIngestionLogExportViewTest(TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._live_diagnostics_patcher = patch(
            "forward_netbox.utilities.execution_ledger.live_support_diagnostics",
            return_value={
                "available": True,
                "source_health": {
                    "available": True,
                    "reachable": True,
                    "checks": [],
                },
                "query_drift": {
                    "available": True,
                    "summary": {"status_counts": {"pass": 1}},
                    "live_summary": {
                        "total_maps": 1,
                        "checked_maps": 1,
                        "warn_count": 0,
                        "info_count": 0,
                        "pass_count": 1,
                        "status_counts": {"live_repository_source_match": 1},
                        "query_id_total": 1,
                        "query_id_pass_count": 1,
                        "query_id_warn_count": 0,
                        "query_id_info_count": 0,
                        "query_id_not_found_count": 0,
                        "query_id_ambiguous_count": 0,
                        "query_id_modified_count": 0,
                        "query_id_unavailable_count": 0,
                        "lookup_error_count": 0,
                        "remediation_action_counts": {},
                        "error": "",
                    },
                    "results": [],
                    "error": "",
                },
                "data_file_health": {
                    "enabled_data_file_map_count": 0,
                    "required_data_files": [],
                    "snapshot_selector": "latestProcessed",
                    "checks": [],
                    "results": [],
                },
                "enabled_map_count": 1,
            },
        )
        cls._view_live_diagnostics_patcher = patch(
            "forward_netbox.views.live_support_diagnostics",
            return_value={
                "available": True,
                "source_health": {
                    "available": True,
                    "reachable": True,
                    "checks": [],
                },
                "query_drift": {
                    "available": True,
                    "summary": {"status_counts": {"pass": 1}},
                    "live_summary": {
                        "total_maps": 1,
                        "checked_maps": 1,
                        "warn_count": 0,
                        "info_count": 0,
                        "pass_count": 1,
                        "status_counts": {"live_repository_source_match": 1},
                        "query_id_total": 1,
                        "query_id_pass_count": 1,
                        "query_id_warn_count": 0,
                        "query_id_info_count": 0,
                        "query_id_not_found_count": 0,
                        "query_id_ambiguous_count": 0,
                        "query_id_modified_count": 0,
                        "query_id_unavailable_count": 0,
                        "lookup_error_count": 0,
                        "remediation_action_counts": {},
                        "error": "",
                    },
                    "results": [],
                    "error": "",
                },
                "data_file_health": {
                    "enabled_data_file_map_count": 0,
                    "required_data_files": [],
                    "snapshot_selector": "latestProcessed",
                    "checks": [],
                    "results": [],
                },
                "enabled_map_count": 1,
            },
        )
        cls._live_diagnostics_patcher.start()
        cls._view_live_diagnostics_patcher.start()

    @classmethod
    def tearDownClass(cls):
        cls._live_diagnostics_patcher.stop()
        cls._view_live_diagnostics_patcher.stop()
        super().tearDownClass()

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
            model_results=[
                {
                    "model": "ipam.prefix",
                    "query_name": "Forward Prefixes",
                    "execution_mode": "query_path",
                    "fetch_mode": "nqe_parameters",
                    "row_count": 1,
                    "delete_count": 0,
                    "query_path_resolution": {
                        "available": True,
                        "query_path_spec_count": 1,
                        "artifact_hit_count": 1,
                        "client_resolve_count": 0,
                        "repository_index_count": 1,
                        "cache_hit_rate": 1.0,
                    },
                }
            ],
        )
        cls.branch = Branch.objects.create(
            name="log-export-field-summary",
            schema_id="log_export_field_summary",
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
                "forward_api_usage": {
                    "api_requests_per_minute": 1800,
                    "http_attempts": 11,
                    "http_429_failures": 0,
                    "nqe_query_calls": 3,
                    "nqe_diff_calls": 2,
                    "nqe_pages": 5,
                    "throttle_sleep_seconds": 0.75,
                    "usage_window_seconds": 30.0,
                    "observed_http_attempts_per_minute": 20.0,
                },
                "statistics": {
                    "dcim.site": {"current": 1, "total": 1},
                    "dcim.cable": {
                        "current": 4,
                        "total": 4,
                        "applied": 1,
                        "failed": 0,
                        "skipped": 0,
                        "unchanged": 2,
                    },
                },
                "dependency_lookup_cache": {
                    "available": True,
                    "row_count": 4,
                    "primed_target_count": 7,
                    "model_count": 1,
                    "models": [
                        {
                            "model": "dcim.device",
                            "row_count": 4,
                            "primed_target_count": 7,
                            "device_name_count": 4,
                            "tag_row_count": 0,
                            "interface_pair_count": 2,
                            "module_bay_pair_count": 0,
                            "fhrp_group_count": 1,
                            "ipam_identity_row_count": 0,
                            "ipam_global_host_row_count": 0,
                        }
                    ],
                },
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
        cls.ingestion.branch = cls.branch
        cls.ingestion.save(update_fields=["job", "merge_job", "branch"])
        prefix_type = ObjectType.objects.get(app_label="ipam", model="prefix")
        ChangeDiff.objects.create(
            branch=cls.branch,
            object_type=prefix_type,
            object_id=1001,
            object_repr="192.0.2.0/27",
            action=ObjectChangeActionChoices.ACTION_UPDATE,
            original={
                "prefix": "192.0.2.0/27",
                "vrf": 100,
                "status": "active",
                "last_updated": "2026-06-03T19:00:00Z",
            },
            modified={
                "prefix": "192.0.2.0/27",
                "vrf": 200,
                "status": "active",
                "last_updated": "2026-06-03T19:05:00Z",
            },
            current={},
            conflicts=[],
        )

    def test_ingestion_detail_renders_change_explainability(self):
        self.client.force_login(self.user)

        response = self.client.get(self.ingestion.get_absolute_url())

        self.assertEqual(
            response.status_code,
            200,
            response.content.decode("utf-8", errors="replace"),
        )
        self.assertContains(response, "Change Explainability")
        self.assertContains(response, "ipam.prefix 1")
        self.assertContains(response, "vrf 1")

    def test_ingestion_poll_refreshes_change_explainability(self):
        self.client.force_login(self.user)

        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardingestion_logs",
                kwargs={"pk": self.ingestion.pk},
            ),
            headers={"HX-Request": "true"},
        )

        self.assertEqual(
            response.status_code,
            200,
            response.content.decode("utf-8", errors="replace"),
        )
        self.assertContains(response, 'id="change_explainability"')
        self.assertContains(response, "ipam.prefix 1")
        self.assertContains(response, "vrf 1")

    def test_poll_defers_change_explainability_while_running(self):
        # Regression (504 fix): while the job is running, the 5s/15s poll must
        # NOT recompute change_explainability — doing so on every poll piles DB
        # load onto the web workers during a long settling merge, which is what
        # produces the gateway 504s on large platform-reclassification syncs.
        running_sync = ForwardSync.objects.create(
            name="sync-running-poll",
            source=self.source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        running_ing = ForwardIngestion.objects.create(
            sync=running_sync,
            snapshot_selector="latestProcessed",
            snapshot_id="snap-running",
        )
        now = timezone.now()
        running_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardIngestion),
            object_id=running_ing.pk,
            name="running-ingestion-job",
            user=None,
            status=JobStatusChoices.STATUS_RUNNING,
            job_id="333e4567-e89b-12d3-a456-426614174002",
            created=now,
            started=now,
            completed=None,
            data={},
        )
        running_ing.job = running_job
        running_ing.save(update_fields=["job"])

        self.client.force_login(self.user)
        with patch("forward_netbox.views.change_explainability_summary") as mock_ce:
            response = self.client.get(
                reverse(
                    "plugins:forward_netbox:forwardingestion_logs",
                    kwargs={"pk": running_ing.pk},
                ),
                headers={"HX-Request": "true"},
            )
        self.assertEqual(response.status_code, 200)
        mock_ce.assert_not_called()

    def test_poll_computes_change_explainability_when_done(self):
        # The completed ingestion still computes it (the deferral is only while
        # the job runs), so the operator sees the breakdown once it finishes.
        self.client.force_login(self.user)
        with patch(
            "forward_netbox.views.change_explainability_summary",
            return_value={"available": False},
        ) as mock_ce:
            self.client.get(
                reverse(
                    "plugins:forward_netbox:forwardingestion_logs",
                    kwargs={"pk": self.ingestion.pk},
                ),
                headers={"HX-Request": "true"},
            )
        mock_ce.assert_called_once()

    def test_export_logs_compacts_large_execution_plan_items(self):
        sync = ForwardSync.objects.create(
            name="sync-log-export-plan-items",
            source=self.source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        ingestion = ForwardIngestion.objects.create(
            sync=sync,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-2",
        )
        sync.set_branch_run_state(
            {
                "snapshot_id": "snapshot-2",
                "phase": "planning",
                "total_plan_items": 99,
                "plan_items": [
                    {
                        "index": index,
                        "status": "queued",
                        "model": "dcim.device",
                    }
                    for index in range(99)
                ],
            }
        )

        self.client.force_login(self.user)

        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardingestion_export_logs",
                kwargs={"pk": ingestion.pk},
            )
        )

        self.assertEqual(
            response.status_code,
            200,
            response.content.decode("utf-8", errors="replace"),
        )
        data = json.loads(response.content)
        self.assertEqual(data["execution_plan"]["total_plan_items"], 99)
        self.assertEqual(data["execution_plan"]["plan_items_count"], 99)
        self.assertTrue(data["execution_plan"]["plan_items_truncated"])
        self.assertEqual(len(data["execution_plan"]["plan_items"]), 25)
        self.assertEqual(
            data["sync"]["execution_state"]["plan_items_count"],
            99,
        )
        self.assertEqual(
            len(data["sync"]["execution_state"]["plan_items"]),
            25,
        )

    def test_ingestion_views_compact_execution_state_for_large_plan_items(self):
        legacy_sync = ForwardSync.objects.create(
            name="sync-log-export-legacy-state",
            source=self.source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        legacy_ingestion = ForwardIngestion.objects.create(
            sync=legacy_sync,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-3",
        )
        legacy_sync.set_branch_run_state(
            {
                "phase": "planning",
                "total_plan_items": 99,
                "next_plan_index": 2,
                "plan_items": [
                    {
                        "index": index,
                        "status": "queued",
                        "model": "dcim.device",
                    }
                    for index in range(99)
                ],
            }
        )

        self.client.force_login(self.user)

        detail_response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardingestion",
                kwargs={"pk": legacy_ingestion.pk},
            )
        )
        logs_response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardingestion_logs",
                kwargs={"pk": legacy_ingestion.pk},
            )
        )
        progress_response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardingestion_progress",
                kwargs={"pk": legacy_ingestion.pk},
            )
        )

        self.assertEqual(detail_response.status_code, 200)
        self.assertEqual(logs_response.status_code, 200)
        self.assertEqual(progress_response.status_code, 200)

        for response in [detail_response, logs_response, progress_response]:
            execution_state = response.context["execution_state"]
            self.assertEqual(execution_state["plan_items_count"], 99)
            self.assertTrue(execution_state["plan_items_truncated"])
            self.assertEqual(len(execution_state["plan_items"]), 25)

    def test_sync_support_bundle_compacts_advisory_workload_preview_plan_items(self):
        standalone_sync = ForwardSync.objects.create(
            name="sync-support-bundle-compact",
            source=self.source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        standalone_sync.set_branch_run_state(
            {
                "snapshot_id": "snapshot-state",
                "phase": "executing",
                "plan_items": [
                    {"index": index, "status": "queued"} for index in range(150)
                ],
            }
        )
        self.client.force_login(self.user)

        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardsync_support_bundle",
                kwargs={"pk": standalone_sync.pk},
            )
        )

        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        advisory = data["sync"]["advisory_summary"]
        self.assertNotIn("plan_items", advisory["branch_run"])
        self.assertEqual(advisory["branch_run"]["plan_items_count"], 150)
