import json
from unittest.mock import patch

from core.choices import JobStatusChoices
from core.choices import ObjectChangeActionChoices
from core.models import Job
from core.models import ObjectType
from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.http import HttpResponse
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone
from netbox_branching.models import Branch
from netbox_branching.models import ChangeDiff
from users.models import ObjectPermission

from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardOwnershipReconciliation
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.diagnostics import SAFE_FAILURE_LOG_PREFIX
from forward_netbox.utilities.diagnostics import SAFE_WARNING_LOG_MESSAGE


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
        cls.change_diff = ChangeDiff.objects.create(
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

    def test_ingestion_diagnostic_routes_require_object_view_permission(self):
        user = get_user_model().objects.create_user(username="ingestion-log-user")
        self.client.force_login(user)
        urls = (
            reverse(
                "plugins:forward_netbox:forwardingestion_logs",
                kwargs={"pk": self.ingestion.pk},
            ),
            reverse(
                "plugins:forward_netbox:forwardingestion_progress",
                kwargs={"pk": self.ingestion.pk},
            ),
            reverse(
                "plugins:forward_netbox:forwardingestion_change_diff",
                kwargs={"pk": self.ingestion.pk, "change_pk": self.change_diff.pk},
            ),
        )

        for url in urls:
            with self.subTest(url=url):
                response = self.client.get(url, headers={"HX-Request": "true"})
                self.assertEqual(response.status_code, 403)

        permission = ObjectPermission.objects.create(
            name="View Forward ingestion diagnostics",
            actions=["view"],
        )
        permission.object_types.add(ObjectType.objects.get_for_model(ForwardIngestion))
        permission.users.add(user)
        for url in urls:
            with self.subTest(url=url):
                response = self.client.get(url, headers={"HX-Request": "true"})
                self.assertEqual(response.status_code, 200)

    def test_ingestion_diagnostics_redact_historical_exception_content(self):
        sentinel = "sentinel-private-detail"
        self.merge_job.data = {
            "error": sentinel,
            "traceback": sentinel,
            "worker_terminal_error": sentinel,
            "error_type": "RuntimeError",
            "logs": [
                [
                    timezone.now().isoformat(),
                    "warning",
                    "sync-log-export",
                    "",
                    sentinel,
                ]
            ],
        }
        self.merge_job.log_entries = [
            {
                "timestamp": timezone.now(),
                "level": "error",
                "message": sentinel,
            }
        ]
        self.merge_job.save(update_fields=["data", "log_entries"])
        self.client.force_login(self.user)

        log_response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardingestion_logs",
                kwargs={"pk": self.ingestion.pk},
            ),
            {"stage": "merge"},
            headers={"HX-Request": "true"},
        )
        export_response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardingestion_export_logs",
                kwargs={"pk": self.ingestion.pk},
            ),
            {"stage": "merge"},
        )

        self.assertEqual(log_response.status_code, 200)
        self.assertEqual(export_response.status_code, 200)
        self.assertNotIn(sentinel, log_response.content.decode())
        self.assertNotIn(sentinel, export_response.content.decode())
        # The row this test writes is a WARNING, so the replacement must not
        # claim an operation failed. A redacted warning used to render the
        # failure sentence, and a deployment's bundle showed thirty of them
        # seconds into a healthy run - routine preflight notices, each pointing
        # at failure evidence that was empty because nothing had failed.
        self.assertContains(log_response, SAFE_WARNING_LOG_MESSAGE)
        self.assertNotContains(log_response, SAFE_FAILURE_LOG_PREFIX)

    def test_sync_support_bundle_exposes_only_job_error_type(self):
        sentinel = "sentinel-private-job-detail"
        self.job.error = "Forward sync failed (ForwardSyncError)."
        self.job.data = {**self.job.data, "error": sentinel}
        self.job.save(update_fields=["error", "data"])
        self.merge_job.error = sentinel
        self.merge_job.save(update_fields=["error"])
        self.client.force_login(self.user)

        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardsync_support_bundle",
                kwargs={"pk": self.sync.pk},
            )
        )

        self.assertEqual(response.status_code, 200)
        payload = json.loads(response.content)
        self.assertEqual(
            payload["latest_ingestion"]["job"]["error"],
            "Job failed (ForwardSyncError).",
        )
        self.assertEqual(
            payload["latest_ingestion"]["merge_job"]["error"],
            "<redacted diagnostic>",
        )
        self.assertNotIn(sentinel, response.content.decode())

    def test_sync_support_bundle_reports_type_only_diff_fallback_reason(self):
        sentinel = "customer-query-path"
        self.ingestion.model_results = [
            {
                "model": "dcim.interface",
                "fetch_parameters": {
                    "fallback_reason": (
                        "diff_budget_exceeded:ForwardFetchBudgetExceededError"
                    ),
                    "forward_netbox_shard_keys": [sentinel],
                },
            }
        ]
        self.ingestion.save(update_fields=["model_results"])
        self.client.force_login(self.user)

        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardsync_support_bundle",
                kwargs={"pk": self.sync.pk},
            )
        )

        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        self.assertEqual(
            data["latest_ingestion"]["diff_fallbacks"],
            [
                {
                    "model": "dcim.interface",
                    "reason": ("diff_budget_exceeded:ForwardFetchBudgetExceededError"),
                    "count": 1,
                }
            ],
        )
        self.assertNotIn(sentinel, response.content.decode())

    def test_support_bundle_password_is_post_only(self):
        url = reverse(
            "plugins:forward_netbox:forwardsync_support_bundle_zip",
            kwargs={"pk": self.sync.pk},
        )
        self.client.force_login(self.user)
        sentinel = "sentinel-private-detail"

        with patch("forward_netbox.views.support_bundle_zip_response") as archive:
            get_response = self.client.get(url, {"password": sentinel})
            archive.assert_not_called()

            archive.return_value = HttpResponse(
                b"archive",
                content_type="application/zip",
            )
            post_response = self.client.post(url, {"password": sentinel})

        self.assertEqual(get_response.status_code, 400)
        self.assertNotIn(sentinel, get_response.content.decode())
        self.assertEqual(post_response.status_code, 200)
        self.assertEqual(archive.call_args.kwargs["password"], sentinel)

    def test_change_diff_route_rejects_change_from_another_ingestion(self):
        other_branch = Branch.objects.create(
            name="log-export-other-branch",
            schema_id="log_export_other_branch",
        )
        other_ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            branch=other_branch,
        )
        other_change = ChangeDiff.objects.create(
            branch=other_branch,
            object_type=ObjectType.objects.get(app_label="ipam", model="prefix"),
            object_id=2002,
            object_repr="198.51.100.0/27",
            action=ObjectChangeActionChoices.ACTION_UPDATE,
            original={"prefix": "198.51.100.0/27", "status": "active"},
            modified={"prefix": "198.51.100.0/27", "status": "reserved"},
            current={},
            conflicts=[],
        )
        self.client.force_login(self.user)

        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardingestion_change_diff",
                kwargs={
                    "pk": self.ingestion.pk,
                    "change_pk": other_change.pk,
                },
            ),
            headers={"HX-Request": "true"},
        )

        self.assertEqual(response.status_code, 404)
        self.assertNotEqual(other_ingestion.pk, self.ingestion.pk)

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

    def test_poll_marks_change_explainability_unavailable_while_running(self):
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
        # The completed ingestion still computes it (it is unavailable only while
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

    def test_sync_support_bundle_uses_aggregate_upgrade_evidence(self):
        standalone_sync = ForwardSync.objects.create(
            name="sync-support-bundle-compact",
            source=self.source,
            parameters={"snapshot_id": "latestProcessed"},
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
        reconciliation = data["upgrade_reconciliation"]
        self.assertTrue(reconciliation["read_only"])
        self.assertNotIn("sample", reconciliation["stale_endpoint_device_types"])
        self.assertNotIn(
            "protected_sample",
            reconciliation["dlm"]["software_versions"],
        )
        self.assertNotIn(
            "unreferenced_sample",
            reconciliation["dlm"]["software_versions"],
        )

    def test_sync_support_bundle_includes_redacted_effective_scope_configuration(self):
        self.source.parameters = {
            **self.source.parameters,
            "sync_endpoints": True,
            "sync_generic_endpoints": False,
            "scope_endpoints_by_include_tags": True,
            "apply_device_scope_tags": True,
            "sync_device_tags": ["private-feature-tag"],
            "device_tag_include_tags": ["private-include-tag"],
            "device_tag_exclude_tags": ["private-exclude-tag"],
            "device_tag_include_match": "all",
            "device_tag_filter_mode": "local",
            "device_tag_prune_out_of_scope": True,
        }
        self.source.save(update_fields=["parameters"])
        standalone_sync = ForwardSync.objects.create(
            name="sync-support-bundle-scope-config",
            source=self.source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        self.client.force_login(self.user)

        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardsync_support_bundle",
                kwargs={"pk": standalone_sync.pk},
            )
        )

        self.assertEqual(response.status_code, 200)
        payload = json.loads(response.content)
        scope = payload["sync"]["scope_configuration"]
        self.assertEqual(
            scope,
            {
                "sync_endpoints": True,
                "sync_generic_endpoints": False,
                "scope_endpoints_by_include_tags": True,
                "apply_device_scope_tags": True,
                "sync_device_tag_count": 1,
                "include_tag_count": 1,
                "exclude_tag_count": 1,
                "include_match": "all",
                "filter_mode": "local",
                "prune_out_of_scope": True,
            },
        )
        rendered = response.content.decode()
        self.assertNotIn("private-feature-tag", rendered)
        self.assertNotIn("private-include-tag", rendered)
        self.assertNotIn("private-exclude-tag", rendered)

    def test_sync_support_bundle_includes_dependency_preview_convergence_evidence(self):
        standalone_sync = ForwardSync.objects.create(
            name="sync-support-bundle-preview",
            source=self.source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        ingestion = ForwardIngestion.objects.create(
            sync=standalone_sync,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-1",
            baseline_ready=True,
            applied_change_count=7,
            failed_change_count=0,
            created_change_count=2,
            updated_change_count=4,
            deleted_change_count=1,
        )
        completed_at = timezone.now()
        ingestion.job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardIngestion),
            object_id=ingestion.pk,
            name="completed support-bundle sync",
            status=JobStatusChoices.STATUS_COMPLETED,
            job_id="123e4567-e89b-12d3-a456-426614174061",
            created=completed_at,
            started=completed_at,
            completed=completed_at,
        )
        ingestion.save(update_fields=["job"])
        ForwardOwnershipReconciliation.objects.create(
            sync=standalone_sync,
            domain=ForwardOwnershipReconciliation.Domain.VIRTUAL_PARENTS,
            generation=ingestion.pk,
            snapshot_id=ingestion.snapshot_id,
            status=ForwardOwnershipReconciliation.Status.COMPLETED,
        )
        Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=standalone_sync.pk,
            name="dependency preview",
            status=JobStatusChoices.STATUS_COMPLETED,
            job_id="123e4567-e89b-12d3-a456-426614174060",
            data={
                "generated_at": "2026-07-18T12:00:00+00:00",
                "context": {
                    "network_id": "must-not-export",
                    "snapshot_id": "snapshot-1",
                    "snapshot_selector": "latestProcessed",
                },
                "change_estimate_kind": "workload_upper_bound",
                "forward_api_usage": {
                    "nqe_query_calls": 34,
                    "nqe_repeated_execution_count": 0,
                    "http_failures": 0,
                },
                "model_results": [
                    {
                        "model": "dcim.device",
                        "row_count": 3,
                        "estimated_changes": 3,
                        "delete_count": 0,
                        "change_estimate_kind": "workload_upper_bound",
                    }
                ],
            },
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
        preview = data["latest_dependency_preview"]
        self.assertEqual(preview["context"]["snapshot_id"], "snapshot-1")
        self.assertNotIn("network_id", preview["context"])
        self.assertEqual(
            preview["latest_sync_evidence"]["status"],
            "ownership_incomplete",
        )
        self.assertEqual(preview["latest_sync_evidence"]["applied"], 7)
        self.assertEqual(
            preview["forward_api_usage"],
            {
                "nqe_query_calls": 34,
                "nqe_repeated_execution_count": 0,
                "http_failures": 0,
            },
        )

    def test_sync_support_bundle_rejects_empty_dependency_preview(self):
        standalone_sync = ForwardSync.objects.create(
            name="sync-support-bundle-empty-preview",
            source=self.source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=standalone_sync.pk,
            name="dependency preview",
            status=JobStatusChoices.STATUS_COMPLETED,
            job_id="123e4567-e89b-12d3-a456-426614174062",
            data={},
        )
        self.client.force_login(self.user)

        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardsync_support_bundle",
                kwargs={"pk": standalone_sync.pk},
            )
        )

        self.assertEqual(response.status_code, 200)
        self.assertIsNone(json.loads(response.content)["latest_dependency_preview"])


class SupportBundleTroubleshootingDepthTest(TestCase):
    """What we can diagnose from a bundle when the operator cannot fix it.

    The GUI now answers every operator question, but the bundle is what reaches
    us when the answer is "this needs the vendor". It carried the ingestion and
    merge jobs and nothing about the scope panel, the operator buttons, or the
    ownership rows that make a device refuse a delete.
    """

    @classmethod
    def setUpTestData(cls):
        User = get_user_model()
        cls.user = User.objects.create_superuser(
            username="bundle-depth-admin",
            password="TestPassword123!",
            email="bundle@example.com",
        )
        cls.source = ForwardSource.objects.create(
            name="bundle-depth-src",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "network_id": "net-1",
            },
        )
        cls.sync = ForwardSync.objects.create(
            name="bundle-depth-sync",
            source=cls.source,
            parameters={"snapshot_id": "latestProcessed"},
        )

    def _bundle(self):
        self.client.force_login(self.user)
        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardsync_support_bundle",
                kwargs={"pk": self.sync.pk},
            )
        )
        self.assertEqual(response.status_code, 200)
        return json.loads(response.content), response.content.decode()

    def test_a_bare_sync_exports_every_section(self):
        # The bundle must be answerable before anything has run: a deployment
        # that has never completed a sync is exactly the one that needs help.
        payload, _raw = self._bundle()
        for section in (
            "scope_reconciliation",
            "operator_action_jobs",
            "ownership_records",
            "stuck_sync",
        ):
            self.assertIn(section, payload, section)
        self.assertEqual(payload["scope_reconciliation"]["report"], {})
        self.assertEqual(payload["ownership_records"]["device_identities"], 0)

    def test_every_operator_button_has_a_slot(self):
        # Derived from BUTTON_JOB_SPECS, so a new button cannot be added
        # without its last run reaching us.
        from forward_netbox.utilities.sync_facade import BUTTON_JOB_SPECS

        payload, _raw = self._bundle()
        self.assertEqual(set(payload["operator_action_jobs"]), set(BUTTON_JOB_SPECS))

    def test_a_refused_prune_arrives_with_its_reason(self):
        # The job data is where a prune records what refused it, and none of
        # the button jobs were exported at all.
        now = timezone.now()
        Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name=f"{self.sync.name} - prune uncovered devices",
            status=JobStatusChoices.STATUS_COMPLETED,
            job_id="123e4567-e89b-12d3-a456-426614174501",
            created=now,
            started=now,
            completed=now,
            data={
                "deleted": 0,
                "protected": {"forward_netbox.forwarddeviceidentity": 3},
                "refused": "survivable-shrink",
            },
        )
        payload, _raw = self._bundle()
        prune = payload["operator_action_jobs"]["prune_uncovered"]
        self.assertIsNotNone(prune)
        self.assertEqual(prune["data"]["refused"], "survivable-shrink")
        self.assertEqual(
            prune["data"]["protected"]["forward_netbox.forwarddeviceidentity"], 3
        )

    def test_ownership_rows_are_counted_by_claim_type_without_names(self):
        # The rows behind the ProtectedError a customer hit deleting a device.
        from dcim.models import Device
        from dcim.models import DeviceRole
        from dcim.models import DeviceType
        from dcim.models import Manufacturer
        from dcim.models import Site
        from extras.models import Tag

        from forward_netbox.models import ForwardDeviceAbsence
        from forward_netbox.models import ForwardDeviceIdentity
        from forward_netbox.models import ForwardDeviceTagClaim
        from forward_netbox.models import ForwardIngestion

        ingestion = ForwardIngestion.objects.create(
            sync=self.sync, snapshot_id="snap-depth"
        )
        mfr = Manufacturer.objects.create(name="MfrB", slug="mfr-b")
        dt = DeviceType.objects.create(manufacturer=mfr, model="dt-b", slug="dt-b")
        role = DeviceRole.objects.create(name="RoleB", slug="role-b")
        site = Site.objects.create(name="SiteB", slug="site-b")
        device = Device.objects.create(
            name="secret-device-name", device_type=dt, role=role, site=site
        )
        ForwardDeviceIdentity.objects.create(
            sync=self.sync,
            ingestion=ingestion,
            source_device_key=device.name,
            device=device,
            snapshot_id="snap-depth",
        )
        tag = Tag.objects.create(name="Forward Uncovered", slug="forward-uncovered")
        ForwardDeviceTagClaim.objects.create(
            sync=self.sync,
            ingestion=ingestion,
            device=device,
            tag=tag,
            claim_type=ForwardDeviceTagClaim.ClaimType.UNCOVERED,
            snapshot_id="snap-depth",
        )
        now = timezone.now()
        ForwardDeviceAbsence.objects.create(
            sync=self.sync,
            device=device,
            consecutive_absent_runs=7,
            first_absent_at=now,
            last_absent_at=now,
        )

        payload, raw = self._bundle()
        records = payload["ownership_records"]
        self.assertEqual(records["device_identities"], 1)
        self.assertEqual(records["device_tag_claims_by_type"]["uncovered"], 1)
        self.assertEqual(records["device_absences"], 1)
        # The streak is what the prune quarantine reads, so it has to travel.
        self.assertEqual(records["longest_absence_streak"], 7)
        # Counts, never names: the identity's source key IS the device name.
        self.assertNotIn("secret-device-name", raw)

    def test_the_scope_report_travels_as_numbers_not_names(self):
        now = timezone.now()
        Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name=f"{self.sync.name} - scope reconciliation",
            status=JobStatusChoices.STATUS_COMPLETED,
            job_id="123e4567-e89b-12d3-a456-426614174502",
            created=now,
            started=now,
            completed=now,
            data={
                "netbox_out_of_scope": 105,
                "out_of_scope_sample": ["private-name-1", "private-name-2"],
                "out_of_scope_device_ids": [11, 22, 33],
                "out_of_scope_absence": {
                    "available": True,
                    "absent_from_snapshot": 100,
                    "present_untagged": 5,
                },
                "unmanaged": {
                    "owned_prune_candidates": 100,
                    "owned_untagged_sample": ["private-name-3"],
                    "owned_quarantine": {"held": 2, "required_runs": 3},
                },
                "present_backfilled_detail_sample": [{"name": "private-name-4"}],
                "backfill_reason_breakdown": {"AUTHENTICATION_FAILED": 52},
            },
        )
        payload, raw = self._bundle()
        report = payload["scope_reconciliation"]["report"]
        # The numbers that explain the panel all survive.
        self.assertEqual(report["netbox_out_of_scope"], 105)
        self.assertEqual(report["out_of_scope_absence"]["absent_from_snapshot"], 100)
        self.assertEqual(report["unmanaged"]["owned_prune_candidates"], 100)
        self.assertEqual(report["unmanaged"]["owned_quarantine"]["held"], 2)
        self.assertEqual(
            report["backfill_reason_breakdown"]["AUTHENTICATION_FAILED"], 52
        )
        # Id lists become counts; name samples do not travel at all.
        self.assertEqual(report["out_of_scope_device_ids_count"], 3)
        self.assertNotIn("out_of_scope_sample", report)
        self.assertNotIn("present_backfilled_detail_sample", report)
        for name in (
            "private-name-1",
            "private-name-2",
            "private-name-3",
            "private-name-4",
        ):
            self.assertNotIn(name, raw)

    def test_blocking_issues_are_exported_before_non_blocking_ones(self):
        from forward_netbox.models import ForwardIngestion
        from forward_netbox.models import ForwardIngestionIssue

        ingestion = ForwardIngestion.objects.create(
            sync=self.sync, snapshot_id="snap-issues"
        )
        # Non-blocking rows created FIRST, so pk order alone would truncate the
        # blocking one away on a large ingestion.
        for index in range(3):
            ForwardIngestionIssue.objects.create(
                ingestion=ingestion,
                phase="sync",
                model="dcim.interface",
                exception="ForwardDependencySkipError",
                message=f"deferred {index}",
            )
        ForwardIngestionIssue.objects.create(
            ingestion=ingestion,
            phase="merge",
            model="dcim.device",
            exception="IntegrityError",
            message="the one that matters",
        )
        payload, _raw = self._bundle()
        issues = payload["latest_ingestion_issues"]
        self.assertEqual(issues["total"], 4)
        self.assertEqual(issues["blocking_total"], 1)
        self.assertTrue(issues["issues"][0]["blocking"])
        self.assertEqual(issues["issues"][0]["message"], "the one that matters")

    def test_the_environment_names_the_versions_behind_the_bundle(self):
        # Without it every bundle costs a round trip to ask what is installed,
        # and an optional plugin at an unvalidated version silently disables a
        # whole integration.
        from forward_netbox import NetboxForwardConfig

        payload, _raw = self._bundle()
        environment = payload["environment"]
        self.assertEqual(environment["plugin_version"], NetboxForwardConfig.version)
        self.assertTrue(environment["netbox_version"])
        self.assertTrue(environment["python_version"])
        # Every optional integration is named whether installed or not, so
        # "absent" is a fact in the file rather than a missing key.
        self.assertIn("netbox_routing", environment["optional_plugins"])
        self.assertIn(
            "netbox_routing",
            environment["optional_plugin_versions_validated_against"],
        )

    def test_the_delete_blocker_survey_splits_ours_from_other_plugins(self):
        # THE question behind "the uncovered count is not going down": whether
        # the blockers are ours, which the prune releases itself, or another
        # plugin's, which it cannot.
        from dcim.models import Device
        from dcim.models import DeviceRole
        from dcim.models import DeviceType
        from dcim.models import Manufacturer
        from dcim.models import Site

        mfr = Manufacturer.objects.create(name="MfrS", slug="mfr-s")
        dt = DeviceType.objects.create(manufacturer=mfr, model="dt-s", slug="dt-s")
        role = DeviceRole.objects.create(name="RoleS", slug="role-s")
        site = Site.objects.create(name="SiteS", slug="site-s")
        device = Device.objects.create(
            name="survey-device", device_type=dt, role=role, site=site
        )
        now = timezone.now()
        Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name=f"{self.sync.name} - scope reconciliation",
            status=JobStatusChoices.STATUS_COMPLETED,
            job_id="123e4567-e89b-12d3-a456-426614174503",
            created=now,
            started=now,
            completed=now,
            data={
                "out_of_scope_device_ids": [device.pk],
                "unmanaged": {"owned_untagged_device_ids": [device.pk]},
            },
        )

        with patch(
            "forward_netbox.utilities.workload_state.describe_delete_blockers",
            return_value=[
                ("netbox_routing.BGPPeer", 10),
                ("forward_netbox.ForwardDeviceIdentity", 1),
            ],
        ):
            payload, raw = self._bundle()

        survey = payload["delete_blockers"]
        # The device appears in both id lists and must be surveyed once.
        self.assertEqual(survey["target_total"], 1)
        self.assertEqual(survey["sampled"], 1)
        self.assertFalse(survey["truncated"])
        self.assertEqual(survey["devices_with_blockers"], 1)
        self.assertEqual(
            survey["other_plugins"]["netbox_routing.BGPPeer"],
            {"rows": 10, "devices": 1},
        )
        self.assertEqual(
            survey["forward_owned"]["forward_netbox.ForwardDeviceIdentity"],
            {"rows": 1, "devices": 1},
        )
        self.assertNotIn("survey-device", raw)

    def test_the_survey_admits_its_bounds(self):
        # A survey that looks exhaustive and is not would have us conclude a
        # blocker is systemic from a capped sample.
        from forward_netbox import views

        now = timezone.now()
        Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name=f"{self.sync.name} - scope reconciliation",
            status=JobStatusChoices.STATUS_COMPLETED,
            job_id="123e4567-e89b-12d3-a456-426614174504",
            created=now,
            started=now,
            completed=now,
            data={
                "out_of_scope_device_ids": list(
                    range(1, views._SUPPORT_BUNDLE_BLOCKER_SAMPLE + 6)
                )
            },
        )
        payload, _raw = self._bundle()
        survey = payload["delete_blockers"]
        self.assertEqual(
            survey["target_total"], views._SUPPORT_BUNDLE_BLOCKER_SAMPLE + 5
        )
        self.assertTrue(survey["truncated"])
        self.assertFalse(survey["stopped_on_time_budget"])

    def test_the_survey_gives_up_before_the_bundle_times_out(self):
        # The bundle is a synchronous GET and the cost per device is the size
        # of its cascade. A partial survey that says so beats a 504.
        from dcim.models import Device
        from forward_netbox import views
        from dcim.models import DeviceRole
        from dcim.models import DeviceType
        from dcim.models import Manufacturer
        from dcim.models import Site

        mfr = Manufacturer.objects.create(name="MfrT", slug="mfr-t")
        dt = DeviceType.objects.create(manufacturer=mfr, model="dt-t", slug="dt-t")
        role = DeviceRole.objects.create(name="RoleT", slug="role-t")
        site = Site.objects.create(name="SiteT", slug="site-t")
        pks = [
            Device.objects.create(
                name=f"slow-{index}", device_type=dt, role=role, site=site
            ).pk
            for index in range(3)
        ]
        now = timezone.now()
        Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name=f"{self.sync.name} - scope reconciliation",
            status=JobStatusChoices.STATUS_COMPLETED,
            job_id="123e4567-e89b-12d3-a456-426614174505",
            created=now,
            started=now,
            completed=now,
            data={"out_of_scope_device_ids": pks},
        )

        # A budget already spent when the loop starts: the first device is
        # surveyed, the rest are declined and the bundle says so.
        with patch.object(views, "_SUPPORT_BUNDLE_BLOCKER_BUDGET_SECONDS", -1.0):
            payload, _raw = self._bundle()

        survey = payload["delete_blockers"]
        self.assertEqual(survey["target_total"], 3)
        self.assertEqual(survey["sampled"], 0)
        self.assertTrue(survey["stopped_on_time_budget"])
        self.assertTrue(survey["truncated"])
