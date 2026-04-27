from unittest.mock import patch

from django.contrib.contenttypes.models import ContentType
from django.test import TestCase

from forward_netbox.choices import ForwardSourceDeploymentChoices
from forward_netbox.exceptions import ForwardConnectivityError
from forward_netbox.exceptions import ForwardSyncError
from forward_netbox.forms import ForwardNQEMapForm
from forward_netbox.forms import ForwardSourceForm
from forward_netbox.forms import ForwardSyncForm
from forward_netbox.models import ForwardSource
from forward_netbox.utilities.forward_api import LATEST_PROCESSED_SNAPSHOT


class ForwardSourceFormTest(TestCase):
    def _base_form_data(self):
        return {
            "name": "source-1",
            "type": ForwardSourceDeploymentChoices.SAAS,
            "username": "user@example.com",
            "password": "secret",
            "network_id": "test-network",
            "timeout": 1200,
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


class ForwardSyncFormTest(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="source-1",
            type=ForwardSourceDeploymentChoices.SAAS,
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "network_id": "test-network",
            },
        )

    def test_form_preserves_auto_merge_and_forces_native_branching(self):
        form = ForwardSyncForm(
            data={
                "name": "sync-1",
                "source": self.source.pk,
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": "on",
                "auto_merge": "",
                "max_changes_per_branch": "10000",
            }
        )

        self.assertTrue(form.is_valid(), form.errors)
        self.assertTrue(form.instance.parameters["multi_branch"])
        self.assertFalse(form.instance.parameters["auto_merge"])
        self.assertFalse(form.instance.auto_merge)
        self.assertEqual(form.instance.parameters["max_changes_per_branch"], 10000)


class ForwardNQEMapFormTest(TestCase):
    def test_coalesce_fields_are_not_normal_form_fields(self):
        form = ForwardNQEMapForm()

        self.assertNotIn("coalesce_fields", form.fields)

    def test_form_defaults_coalesce_fields_from_model_contract(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="site")
        form = ForwardNQEMapForm(
            data={
                "name": "site-map",
                "netbox_model": netbox_model.pk,
                "query": 'select {\n  name: "site-a",\n  slug: "site-a"\n}',
                "enabled": "on",
                "weight": "100",
            }
        )

        self.assertTrue(form.is_valid(), form.errors)
        instance = form.save(commit=False)
        instance.clean()
        self.assertEqual(instance.coalesce_fields, [["slug"], ["name"]])
