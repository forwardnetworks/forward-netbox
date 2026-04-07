from unittest.mock import Mock
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase
from rest_framework.test import APIRequestFactory
from rest_framework.test import force_authenticate

from forward_netbox.api.views import ForwardSourceViewSet
from forward_netbox.exceptions import ForwardConnectivityError
from forward_netbox.exceptions import ForwardSyncError


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
