from unittest.mock import Mock
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.test import TestCase
from rest_framework.test import APIRequestFactory
from rest_framework.test import force_authenticate

from forward_netbox.api.views import ForwardNQEMapViewSet
from forward_netbox.api.views import ForwardSourceViewSet
from forward_netbox.api.views import ForwardValidationRunViewSet
from forward_netbox.choices import ForwardValidationStatusChoices
from forward_netbox.exceptions import ForwardConnectivityError
from forward_netbox.exceptions import ForwardSyncError
from forward_netbox.models import ForwardDriftPolicy
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
        mock_client.get_nqe_repository_queries.return_value = [
            {
                "queryId": "FQ_devices",
                "path": "/Library/NetBox/forward_devices",
                "repository": "fwd",
            },
        ]
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
        mock_client.get_nqe_repository_queries.assert_called_once_with(
            repository="fwd",
            directory="/",
        )

    @patch("forward_netbox.api.views.ForwardSource.get_client")
    def test_available_queries_returns_repository_scoped_query_choices(
        self, mock_get_client
    ):
        mock_client = Mock()
        mock_client.get_nqe_repository_queries.return_value = [
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
        mock_client.get_nqe_repository_queries.assert_called_once_with(
            repository="org", directory="/forward_netbox_validation/"
        )

    @patch("forward_netbox.api.views.ForwardSource.get_client")
    def test_available_queries_filters_by_netbox_model(self, mock_get_client):
        mock_client = Mock()
        mock_client.get_nqe_repository_queries.return_value = [
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
        mock_client.get_nqe_repository_queries.return_value = [
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
        mock_client.resolve_nqe_query_reference.return_value = {
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
        mock_client.resolve_nqe_query_reference.assert_called_once_with(
            repository="org",
            query_path="/forward_netbox_validation/forward_interfaces",
        )
        mock_client.get_nqe_query_history.assert_called_once_with("Q_interfaces")

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
