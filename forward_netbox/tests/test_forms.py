from unittest.mock import patch

from django.test import TestCase

from forward_netbox.choices import ForwardSourceDeploymentChoices
from forward_netbox.exceptions import ForwardConnectivityError
from forward_netbox.exceptions import ForwardSyncError
from forward_netbox.forms import ForwardSourceForm


class ForwardSourceFormTest(TestCase):
    def _base_form_data(self):
        return {
            "name": "source-1",
            "type": ForwardSourceDeploymentChoices.SAAS,
            "username": "user@example.com",
            "password": "secret",
            "network_id": "235937",
            "timeout": 60,
            "verify": True,
        }

    @patch("forward_netbox.forms.ForwardSource.validate_connection")
    def test_requires_network_id(self, mock_validate_connection):
        data = self._base_form_data()
        data["network_id"] = ""
        form = ForwardSourceForm(data=data)

        self.assertFalse(form.is_valid())
        self.assertIn(
            "Select a Forward network for this source.",
            form.non_field_errors(),
        )

    @patch(
        "forward_netbox.forms.ForwardSource.validate_connection",
        side_effect=ForwardSyncError(
            "Forward API request failed with HTTP 401: unauthorized"
        ),
    )
    def test_shows_authentication_message_for_401(self, _mock_validate_connection):
        form = ForwardSourceForm(data=self._base_form_data())

        self.assertFalse(form.is_valid())
        self.assertIn(
            "Could not authenticate to Forward.",
            " ".join(form.non_field_errors()),
        )

    @patch(
        "forward_netbox.forms.ForwardSource.validate_connection",
        side_effect=ForwardConnectivityError(
            "Could not connect to Forward API endpoint: connection timeout"
        ),
    )
    def test_shows_connectivity_message(self, _mock_validate_connection):
        form = ForwardSourceForm(data=self._base_form_data())

        self.assertFalse(form.is_valid())
        self.assertIn(
            "Could not connect to Forward. Verify the Forward URL and network connectivity",
            " ".join(form.non_field_errors()),
        )
