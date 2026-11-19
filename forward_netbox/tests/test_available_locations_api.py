# The site rules form offers the Forward location names as suggestions for a location
# rule, from the same kind of endpoint that lists the tags for a tag rule.
from unittest.mock import Mock
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase
from rest_framework.test import APIRequestFactory
from rest_framework.test import force_authenticate

from forward_netbox.api.views import ForwardSourceViewSet
from forward_netbox.exceptions import ForwardSyncError
from forward_netbox.models import ForwardSource


class AvailableLocationsTest(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.user = get_user_model().objects.create_superuser(
            username="loc_admin", password="TestPassword123!", email="a@example.com"
        )
        cls.source = ForwardSource.objects.create(
            name="loc-source",
            type="saas",
            url="https://forward.example.test",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "network_id": "net-1",
            },
        )

    def _get(self, params):
        request = APIRequestFactory().get(
            "/api/plugins/forward/source/available-locations/", params
        )
        force_authenticate(request, user=self.user)
        return ForwardSourceViewSet.as_view({"get": "available_locations"})(request)

    def test_it_needs_a_saved_source(self):
        response = self._get({})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["count"], 0)
        self.assertIn("saved Forward source is required", response.data["detail"])
        self.assertIn("locations", response.data["detail"])

    @patch("forward_netbox.api.views.ForwardSource.get_client")
    def test_credentials_in_the_query_string_are_refused(self, mock_get_client):
        response = self._get({"source_id": self.source.pk, "password": "sentinel"})

        self.assertEqual(response.status_code, 400)
        self.assertNotIn("sentinel", str(response.data))
        mock_get_client.assert_not_called()

    @patch("forward_netbox.api.views.run_nqe_query")
    @patch("forward_netbox.api.views.get_latest_processed_snapshot")
    @patch("forward_netbox.api.views.ForwardSource.get_client")
    def test_it_returns_the_distinct_location_names_sorted(
        self, mock_get_client, mock_snapshot, mock_run
    ):
        mock_get_client.return_value = Mock()
        mock_snapshot.return_value = {"id": "snapshot-1"}
        mock_run.return_value = [
            {"name": "Hall B"},
            {"name": "Hall A"},
            {"name": "Hall B"},
            {"name": ""},
            {"name": None},
        ]

        response = self._get({"source_id": self.source.pk, "network_id": "net-1"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            [row["id"] for row in response.data["results"]], ["Hall A", "Hall B"]
        )
        self.assertEqual(response.data["count"], 2)

    @patch("forward_netbox.api.views.run_nqe_query")
    @patch("forward_netbox.api.views.get_latest_processed_snapshot")
    @patch("forward_netbox.api.views.ForwardSource.get_client")
    def test_the_query_asks_for_locations_not_device_tags(
        self, mock_get_client, mock_snapshot, mock_run
    ):
        mock_get_client.return_value = Mock()
        mock_snapshot.return_value = {"id": "snapshot-1"}
        mock_run.return_value = []

        self._get({"source_id": self.source.pk, "network_id": "net-1"})

        query = mock_run.call_args.kwargs["query"]
        self.assertIn("network.locations", query)
        self.assertNotIn("tagNames", query)

    @patch("forward_netbox.api.views.run_nqe_query")
    @patch("forward_netbox.api.views.get_latest_processed_snapshot")
    @patch("forward_netbox.api.views.ForwardSource.get_client")
    def test_q_narrows_the_names_ignoring_case(
        self, mock_get_client, mock_snapshot, mock_run
    ):
        mock_get_client.return_value = Mock()
        mock_snapshot.return_value = {"id": "snapshot-1"}
        mock_run.return_value = [{"name": "Hall A"}, {"name": "Annex"}]

        response = self._get(
            {"source_id": self.source.pk, "network_id": "net-1", "q": "HALL"}
        )

        self.assertEqual([row["id"] for row in response.data["results"]], ["Hall A"])

    @patch("forward_netbox.api.views.get_latest_processed_snapshot")
    @patch("forward_netbox.api.views.ForwardSource.get_client")
    def test_a_forward_error_is_a_message_not_a_failure(
        self, mock_get_client, mock_snapshot
    ):
        mock_get_client.return_value = Mock()
        mock_snapshot.side_effect = ForwardSyncError(
            "Forward API request failed with HTTP 401: unauthorized"
        )

        response = self._get({"source_id": self.source.pk, "network_id": "net-1"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["count"], 0)
        self.assertIn("Could not authenticate", response.data["detail"])

    def test_the_tags_endpoint_still_says_tags(self):
        request = APIRequestFactory().get(
            "/api/plugins/forward/source/available-tags/", {}
        )
        force_authenticate(request, user=self.user)

        response = ForwardSourceViewSet.as_view({"get": "available_tags"})(request)

        self.assertIn("load device tags", response.data["detail"])
