from datetime import timedelta
from unittest.mock import Mock
from unittest.mock import patch
from uuid import uuid4

from core.choices import JobStatusChoices
from core.models import Job
from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.test import TestCase
from django.utils import timezone
from netbox_branching.models import Branch
from rest_framework.test import APIRequestFactory
from rest_framework.test import force_authenticate

from forward_netbox.api.views import ForwardExecutionRunViewSet
from forward_netbox.api.views import ForwardExecutionStepViewSet
from forward_netbox.api.views import ForwardNQEMapViewSet
from forward_netbox.api.views import ForwardSourceViewSet
from forward_netbox.api.views import ForwardValidationRunViewSet
from forward_netbox.choices import ForwardExecutionStepStatusChoices
from forward_netbox.choices import ForwardValidationStatusChoices
from forward_netbox.exceptions import ForwardConnectivityError
from forward_netbox.exceptions import ForwardSyncError
from forward_netbox.models import ForwardDriftPolicy
from forward_netbox.models import ForwardExecutionRun
from forward_netbox.models import ForwardExecutionStep
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.models import ForwardValidationRun
from forward_netbox.utilities.forward_api import LATEST_PROCESSED_SNAPSHOT


class ForwardSourceAPIViewTest(TestCase):
    @classmethod
    def setUpTestData(cls):
        User = get_user_model()
        cls.user = User.objects.create_superuser(
            username="nb_admin",
            password="TestPassword123!",
            email="admin@example.com",
        )

    @staticmethod
    def _invoke(request_user, params):
        factory = APIRequestFactory()
        request = factory.get(
            "/api/plugins/forward/source/available-networks/",
            params,
        )
        force_authenticate(request, user=request_user)
        view = ForwardSourceViewSet.as_view({"get": "available_networks"})
        return view(request)

    @staticmethod
    def _invoke_tags(request_user, params):
        factory = APIRequestFactory()
        request = factory.get(
            "/api/plugins/forward/source/available-tags/",
            params,
        )
        force_authenticate(request, user=request_user)
        view = ForwardSourceViewSet.as_view({"get": "available_tags"})
        return view(request)

    def test_available_networks_requires_forward_credentials(self):
        response = self._invoke(self.user, {"type": "saas"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["count"], 0)
        self.assertIn("Enter Forward username and password", response.data["detail"])

    @patch("forward_netbox.api.views.ForwardSource.get_client")
    def test_available_networks_shows_auth_message_on_401(self, mock_get_client):
        mock_client = Mock()
        mock_client.get_networks.side_effect = ForwardSyncError(
            "Forward API request failed with HTTP 401: unauthorized"
        )
        mock_get_client.return_value = mock_client

        response = self._invoke(
            self.user,
            {
                "type": "saas",
                "username": "user@example.com",
                "password": "secret",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["count"], 0)
        self.assertIn(
            "Could not authenticate to Forward. Verify username and password.",
            response.data["detail"],
        )

    @patch("forward_netbox.api.views.ForwardSource.get_client")
    def test_available_networks_shows_connectivity_message_on_network_error(
        self, mock_get_client
    ):
        mock_client = Mock()
        mock_client.get_networks.side_effect = ForwardConnectivityError(
            "Could not connect to Forward API endpoint: DNS resolution failure"
        )
        mock_get_client.return_value = mock_client

        response = self._invoke(
            self.user,
            {
                "type": "saas",
                "username": "user@example.com",
                "password": "secret",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["count"], 0)
        self.assertIn(
            "Could not contact the Forward API endpoint",
            response.data["detail"],
        )

    def test_available_tags_requires_forward_credentials(self):
        response = self._invoke_tags(self.user, {"type": "saas"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["count"], 0)
        self.assertIn("Enter Forward username and password", response.data["detail"])

    @patch("forward_netbox.api.views.ForwardSource.get_client")
    def test_available_tags_returns_distinct_tags(self, mock_get_client):
        mock_client = Mock()
        mock_client.run_nqe_query.return_value = [
            {"tagNames": ["Core", "Branch"]},
            {"tagNames": ["Core", "Edge"]},
        ]
        mock_get_client.return_value = mock_client

        response = self._invoke_tags(
            self.user,
            {
                "type": "saas",
                "username": "user@example.com",
                "password": "secret",
                "network_id": "net-1",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["count"], 3)
        self.assertEqual(
            [row["id"] for row in response.data["results"]],
            ["Branch", "Core", "Edge"],
        )


class ForwardNQEMapAPIViewTest(TestCase):
    @classmethod
    def setUpTestData(cls):
        User = get_user_model()
        cls.user = User.objects.create_superuser(
            username="nb_admin",
            password="TestPassword123!",
            email="admin@example.com",
        )
        cls.source = ForwardSource.objects.create(
            name="source-1",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
            },
        )

    @staticmethod
    def _invoke(request_user, action, url_path, params):
        factory = APIRequestFactory()
        request = factory.get(
            f"/api/plugins/forward/nqe-map/{url_path}/",
            params,
        )
        force_authenticate(request, user=request_user)
        view = ForwardNQEMapViewSet.as_view({"get": action})
        return view(request)

    def _invoke_available_queries(self, params):
        return self._invoke(
            self.user,
            "available_queries",
            "available-queries",
            params,
        )

    @patch("forward_netbox.api.views.ForwardSource.get_client")
    def test_available_query_folders_returns_detected_hierarchy(self, mock_get_client):
        mock_client = Mock()
        mock_client.get_nqe_repository_query_index.return_value = {
            "rows": [
                {
                    "queryId": "FQ_devices",
                    "path": "/Library/NetBox/forward_devices",
                    "repository": "fwd",
                },
            ]
        }
        mock_get_client.return_value = mock_client

        response = self._invoke(
            self.user,
            "available_query_folders",
            "available-query-folders",
            {
                "source_id": self.source.pk,
                "repository": "fwd",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            [item["id"] for item in response.data["results"]],
            ["/", "/Library/", "/Library/NetBox/"],
        )
        mock_client.get_nqe_repository_query_index.assert_called_once_with(
            repository="fwd",
            directory="/",
        )

    @patch("forward_netbox.api.views.ForwardSource.get_client")
    def test_available_queries_returns_repository_scoped_query_choices(
        self, mock_get_client
    ):
        mock_client = Mock()
        mock_client.get_nqe_repository_query_index.return_value = {
            "rows": [
                {
                    "queryId": "Q_devices",
                    "path": "/forward_netbox_validation/forward_devices",
                    "intent": "Forward Devices",
                },
                {
                    "queryId": "Q_interfaces",
                    "path": "/forward_netbox_validation/forward_interfaces",
                    "intent": "Forward Interfaces",
                },
            ]
        }
        mock_get_client.return_value = mock_client

        response = self._invoke(
            self.user,
            "available_queries",
            "available-queries",
            {
                "source_id": self.source.pk,
                "repository": "org",
                "directory": "/forward_netbox_validation/",
                "q": "interfaces",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["count"], 1)
        self.assertEqual(
            response.data["results"][0]["id"],
            "/forward_netbox_validation/forward_interfaces",
        )
        self.assertEqual(response.data["results"][0]["query_id"], "Q_interfaces")
        self.assertIn("Forward Interfaces", response.data["results"][0]["display"])
        mock_client.get_nqe_repository_query_index.assert_called_once_with(
            repository="org", directory="/forward_netbox_validation/"
        )

    @patch("forward_netbox.api.views.ForwardSource.get_client")
    def test_available_queries_filters_by_netbox_model(self, mock_get_client):
        mock_client = Mock()
        mock_client.get_nqe_repository_query_index.return_value = {
            "rows": [
                {
                    "queryId": "Q_devices",
                    "path": "/forward_netbox_validation/forward_devices",
                    "intent": "Forward Devices",
                },
                {
                    "queryId": "Q_interfaces",
                    "path": "/forward_netbox_validation/forward_interfaces",
                    "intent": "Forward Interfaces",
                },
            ]
        }
        mock_get_client.return_value = mock_client

        response = self._invoke(
            self.user,
            "available_queries",
            "available-queries",
            {
                "source_id": self.source.pk,
                "repository": "org",
                "directory": "/forward_netbox_validation/",
                "model_string": "dcim.device",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["count"], 1)
        self.assertEqual(
            response.data["results"][0]["id"],
            "/forward_netbox_validation/forward_devices",
        )

    @patch("forward_netbox.api.views.ForwardSource.get_client")
    def test_available_queries_filters_by_netbox_model_content_type_id(
        self, mock_get_client
    ):
        mock_client = Mock()
        mock_client.get_nqe_repository_query_index.return_value = {
            "rows": [
                {
                    "queryId": "Q_devices",
                    "path": "/forward_netbox_validation/forward_devices",
                    "intent": "Forward Devices",
                },
                {
                    "queryId": "Q_interfaces",
                    "path": "/forward_netbox_validation/forward_interfaces",
                    "intent": "Forward Interfaces",
                },
            ]
        }
        mock_get_client.return_value = mock_client
        model_pk = ContentType.objects.get(app_label="dcim", model="device").pk

        response = self._invoke(
            self.user,
            "available_queries",
            "available-queries",
            {
                "source_id": self.source.pk,
                "repository": "org",
                "directory": "/forward_netbox_validation/",
                "model_string": model_pk,
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["count"], 1)
        self.assertEqual(
            response.data["results"][0]["id"],
            "/forward_netbox_validation/forward_devices",
        )

    @patch("forward_netbox.api.views.ForwardSource.get_client")
    def test_available_query_commits_returns_history_choices(self, mock_get_client):
        mock_client = Mock()
        mock_client.get_nqe_query_history.return_value = [
            {
                "id": "commit-1",
                "path": "/forward_netbox_validation/forward_interfaces",
                "committedAt": "2026-05-10T12:00:00Z",
                "author": "operator@example.com",
                "message": {"subject": "Update NetBox query"},
            }
        ]
        mock_get_client.return_value = mock_client

        response = self._invoke(
            self.user,
            "available_query_commits",
            "available-query-commits",
            {
                "source_id": self.source.pk,
                "query_id": "Q_interfaces",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["results"][0]["id"], "commit-1")
        self.assertIn("Update NetBox query", response.data["results"][0]["display"])
        mock_client.get_nqe_query_history.assert_called_once_with("Q_interfaces")

    @patch("forward_netbox.api.views.ForwardSource.get_client")
    def test_available_query_commits_resolves_path_before_history(
        self, mock_get_client
    ):
        mock_client = Mock()
        mock_client.get_nqe_repository_query_index.return_value = {
            "by_path": {
                "/forward_netbox_validation/forward_interfaces": {
                    "queryId": "Q_interfaces",
                    "path": "/forward_netbox_validation/forward_interfaces",
                    "lastCommitId": "commit-1",
                }
            }
        }
        mock_client.get_nqe_query_history.return_value = [
            {
                "id": "commit-1",
                "path": "/forward_netbox_validation/forward_interfaces",
                "committedAt": "2026-05-10T12:00:00Z",
            }
        ]
        mock_get_client.return_value = mock_client

        response = self._invoke(
            self.user,
            "available_query_commits",
            "available-query-commits",
            {
                "source_id": self.source.pk,
                "repository": "org",
                "query_path": "/forward_netbox_validation/forward_interfaces",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["results"][0]["id"], "commit-1")
        mock_client.get_nqe_repository_query_index.assert_called_once_with(
            repository="org",
            directory="/",
        )
        mock_client.get_committed_nqe_query.assert_not_called()
        mock_client.get_nqe_query_history.assert_called_once_with("Q_interfaces")

    @patch("forward_netbox.api.views.ForwardSource.get_client")
    def test_available_query_commits_falls_back_to_committed_query_when_index_missing(
        self, mock_get_client
    ):
        mock_client = Mock()
        mock_client.get_nqe_repository_query_index.return_value = {"by_path": {}}
        mock_client.get_committed_nqe_query.return_value = {
            "queryId": "Q_interfaces",
            "commitId": "commit-1",
        }
        mock_client.get_nqe_query_history.return_value = [
            {
                "id": "commit-1",
                "path": "/forward_netbox_validation/forward_interfaces",
                "committedAt": "2026-05-10T12:00:00Z",
            }
        ]
        mock_get_client.return_value = mock_client

        response = self._invoke(
            self.user,
            "available_query_commits",
            "available-query-commits",
            {
                "source_id": self.source.pk,
                "repository": "org",
                "query_path": "/forward_netbox_validation/forward_interfaces",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["results"][0]["id"], "commit-1")
        mock_client.get_nqe_repository_query_index.assert_called_once_with(
            repository="org",
            directory="/",
        )
        mock_client.get_committed_nqe_query.assert_called_once_with(
            repository="org",
            query_path="/forward_netbox_validation/forward_interfaces",
            commit_id="head",
            query_index={"by_path": {}},
        )
        mock_client.get_nqe_query_history.assert_called_once_with("Q_interfaces")

    @patch("forward_netbox.api.views.ForwardSource.get_client")
    def test_available_query_commits_uses_fwd_query_index_before_history(
        self, mock_get_client
    ):
        mock_client = Mock()
        mock_client.get_nqe_repository_query_index.return_value = {
            "by_path": {
                "/netbox/forward_interfaces": {
                    "queryId": "FQ_interfaces",
                    "path": "/netbox/forward_interfaces",
                    "lastCommitId": "commit-1",
                }
            }
        }
        mock_client.get_nqe_query_history.return_value = [
            {
                "id": "commit-1",
                "path": "/netbox/forward_interfaces",
                "committedAt": "2026-05-10T12:00:00Z",
            }
        ]
        mock_get_client.return_value = mock_client

        response = self._invoke(
            self.user,
            "available_query_commits",
            "available-query-commits",
            {
                "source_id": self.source.pk,
                "repository": "fwd",
                "query_path": "/netbox/forward_interfaces",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["results"][0]["id"], "commit-1")
        mock_client.get_nqe_repository_query_index.assert_called_once_with(
            repository="fwd",
            directory="/",
        )
        mock_client.get_committed_nqe_query.assert_not_called()
        mock_client.get_nqe_query_history.assert_called_once_with("FQ_interfaces")

    def test_available_queries_requires_source(self):
        response = self._invoke_available_queries({})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["count"], 0)
        self.assertIn("Select a Forward Source", response.data["detail"])


class ForwardValidationRunAPIViewTest(TestCase):
    @classmethod
    def setUpTestData(cls):
        User = get_user_model()
        cls.user = User.objects.create_superuser(
            username="nb_admin",
            password="TestPassword123!",
            email="admin@example.com",
        )
        cls.source = ForwardSource.objects.create(
            name="source-1",
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
        cls.policy = ForwardDriftPolicy.objects.create(name="policy-1")
        cls.sync = ForwardSync.objects.create(
            name="sync-1",
            source=cls.source,
            drift_policy=cls.policy,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
                "enable_bulk_orm": False,
            },
        )

    def test_force_allow_updates_validation_run(self):
        validation_run = ForwardValidationRun.objects.create(
            sync=self.sync,
            policy=self.policy,
            status=ForwardValidationStatusChoices.BLOCKED,
            allowed=False,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-1",
            blocking_reasons=["Snapshot not processed."],
        )
        factory = APIRequestFactory()
        request = factory.post(
            f"/api/plugins/forward/validation-run/{validation_run.pk}/force_allow/",
            {"reason": "Accepted for lab validation."},
            format="json",
        )
        force_authenticate(request, user=self.user)
        view = ForwardValidationRunViewSet.as_view({"post": "force_allow"})

        response = view(request, pk=validation_run.pk)

        self.assertEqual(response.status_code, 200)
        validation_run.refresh_from_db()
        self.assertTrue(validation_run.override_applied)
        self.assertTrue(validation_run.allowed)
        self.assertEqual(validation_run.status, ForwardValidationStatusChoices.PASSED)
        self.assertEqual(validation_run.override_reason, "Accepted for lab validation.")


class ForwardExecutionRunAPIViewTest(TestCase):
    @classmethod
    def setUpTestData(cls):
        User = get_user_model()
        cls.user = User.objects.create_superuser(
            username="execution_api_admin",
            password="TestPassword123!",
            email="admin@example.com",
        )
        cls.source = ForwardSource.objects.create(
            name="execution-api-source",
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
            name="execution-api-sync",
            source=cls.source,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "enable_bulk_orm": False,
            },
        )
        cls.execution_run = ForwardExecutionRun.objects.create(
            sync=cls.sync,
            source=cls.source,
            backend="branching",
            status="running",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-1",
            total_steps=1,
            next_step_index=1,
        )
        cls.execution_step = ForwardExecutionStep.objects.create(
            run=cls.execution_run,
            index=1,
            status="running",
            model_string="dcim.site",
            label="dcim.site part 1",
            estimated_changes=10,
            fetch_mode="model",
            query_parameters={"forward_netbox_shard_keys": ["device-1"]},
        )

    @staticmethod
    def _invoke(request_user, action, method="get", pk=None):
        factory = APIRequestFactory()
        request_method = getattr(factory, method)
        request = request_method(
            f"/api/plugins/forward/execution-run/1/{action}/",
            {},
            format="json",
        )
        force_authenticate(request, user=request_user)
        view = ForwardExecutionRunViewSet.as_view({method: action})
        return view(request, pk=pk or ForwardExecutionRunAPIViewTest.execution_run.pk)

    def test_support_bundle_returns_execution_steps(self):
        response = self._invoke(self.user, "support_bundle")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["run"]["id"], self.execution_run.pk)
        self.assertEqual(response.data["steps"][0]["id"], self.execution_step.pk)
        self.assertEqual(
            response.data["steps"][0]["query_parameters"],
            {"forward_netbox_shard_keys": ["device-1"]},
        )
        self.assertEqual(
            response.data["steps"][0]["apply_engine_decision"]["selected_engine"],
            "adapter",
        )

    def test_execution_step_detail_exposes_apply_engine_decision(self):
        factory = APIRequestFactory()
        request = factory.get(
            f"/api/plugins/forward/execution-step/{self.execution_step.pk}/"
        )
        force_authenticate(request, user=self.user)
        view = ForwardExecutionStepViewSet.as_view({"get": "retrieve"})

        response = view(request, pk=self.execution_step.pk)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["apply_engine"], "adapter")
        self.assertEqual(
            response.data["query_parameters"],
            {"forward_netbox_shard_keys": ["device-1"]},
        )
        self.assertEqual(
            response.data["apply_engine_decision"]["reason_code"],
            "bulk_orm_disabled_by_default",
        )

    def test_reconcile_endpoint_returns_execution_run(self):
        response = self._invoke(self.user, "reconcile", method="post")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["id"], self.execution_run.pk)
        self.assertIn("support_summary", response.data)

    def test_reconcile_marks_stale_stage_without_branch_retryable(self):
        stale_run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status="running",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-1",
            total_steps=1,
            next_step_index=1,
        )
        stale_step = ForwardExecutionStep.objects.create(
            run=stale_run,
            index=1,
            status=ForwardExecutionStepStatusChoices.RUNNING,
            model_string="dcim.site",
            label="dcim.site part 1",
            heartbeat=timezone.now() - timedelta(minutes=20),
        )

        response = self._invoke(
            self.user,
            "reconcile",
            method="post",
            pk=stale_run.pk,
        )

        self.assertEqual(response.status_code, 200)
        stale_step.refresh_from_db()
        self.assertEqual(stale_step.status, ForwardExecutionStepStatusChoices.PENDING)
        self.assertIn("automatic requeue", stale_step.last_error)

    @patch("forward_netbox.api.views.enqueue_branch_stage_job")
    def test_retry_current_step_refuses_failed_step_with_partial_branch(
        self,
        mock_enqueue,
    ):
        branch = Branch.objects.create(
            name="forward-test-partial-branch",
            schema_id="forward_test_partial_branch",
        )
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-1",
            branch=branch,
        )
        run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status="failed",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-1",
            total_steps=1,
            next_step_index=1,
        )
        ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            status=ForwardExecutionStepStatusChoices.FAILED,
            model_string="dcim.site",
            label="dcim.site part 1",
            ingestion=ingestion,
            branch=branch,
        )

        response = self._invoke(
            self.user,
            "retry_current_step",
            method="post",
            pk=run.pk,
        )

        self.assertEqual(response.status_code, 409)
        mock_enqueue.assert_not_called()

    @patch("forward_netbox.api.views.enqueue_branch_stage_job")
    def test_retry_current_step_refuses_duplicate_queued_retry(
        self,
        mock_enqueue,
    ):
        run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status="running",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-1",
            total_steps=1,
            next_step_index=1,
        )
        step = ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            status=ForwardExecutionStepStatusChoices.QUEUED,
            model_string="dcim.site",
            label="dcim.site part 1",
            retry_count=2,
        )
        self.sync.status = "queued"
        self.sync.save(update_fields=["status"])

        response = self._invoke(
            self.user,
            "retry_current_step",
            method="post",
            pk=run.pk,
        )

        self.assertEqual(response.status_code, 409)
        self.assertIn("already queued", response.data["detail"])
        mock_enqueue.assert_not_called()
        step.refresh_from_db()
        self.assertEqual(step.retry_count, 2)

    @patch("forward_netbox.api.views.enqueue_branch_stage_job")
    def test_discard_branch_retry_deletes_partial_branch_and_queues_retry(
        self,
        mock_enqueue,
    ):
        branch = Branch.objects.create(
            name="forward-test-discard-branch",
            schema_id="forward_test_discard_branch",
        )
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-1",
            branch=branch,
        )
        run = ForwardExecutionRun.objects.create(
            sync=self.sync,
            source=self.source,
            backend="branching",
            status="failed",
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-1",
            total_steps=1,
            next_step_index=1,
        )
        step = ForwardExecutionStep.objects.create(
            run=run,
            index=1,
            status=ForwardExecutionStepStatusChoices.FAILED,
            model_string="dcim.site",
            label="dcim.site part 1",
            ingestion=ingestion,
            branch=branch,
        )
        job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="Retry Forward Branching Shard",
            user=self.user,
            status=JobStatusChoices.STATUS_PENDING,
            job_id=uuid4(),
            created=timezone.now(),
            data={},
        )
        mock_enqueue.return_value = job

        response = self._invoke(
            self.user,
            "discard_branch_retry",
            method="post",
            pk=run.pk,
        )

        self.assertEqual(response.status_code, 201)
        ingestion.refresh_from_db()
        step.refresh_from_db()
        self.assertFalse(Branch.objects.filter(pk=branch.pk).exists())
        self.assertIsNone(ingestion.branch)
        self.assertIsNone(step.branch)
        self.assertEqual(step.status, ForwardExecutionStepStatusChoices.QUEUED)
        self.assertEqual(step.retry_count, 1)
        mock_enqueue.assert_called_once_with(run.sync, user=self.user, adhoc=True)
