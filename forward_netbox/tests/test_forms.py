from unittest.mock import patch

from django.conf import settings
from django.contrib.contenttypes.models import ContentType
from django.test import override_settings
from django.test import TestCase

from forward_netbox.choices import FORWARD_BGP_MODELS
from forward_netbox.choices import ForwardExecutionBackendChoices
from forward_netbox.choices import ForwardSourceDeploymentChoices
from forward_netbox.exceptions import ForwardConnectivityError
from forward_netbox.exceptions import ForwardSyncError
from forward_netbox.forms import FORWARD_NQE_QUERY_REPOSITORY_CHOICES
from forward_netbox.forms import ForwardNQEMapBulkEditForm
from forward_netbox.forms import ForwardNQEMapForm
from forward_netbox.forms import ForwardSourceForm
from forward_netbox.forms import ForwardSyncForm
from forward_netbox.models import ForwardNQEMap
from forward_netbox.models import ForwardSource
from forward_netbox.utilities.forward_api import LATEST_PROCESSED_SNAPSHOT


BGP_PLUGIN_CONFIG = {
    **settings.PLUGINS_CONFIG,
    "forward_netbox": {
        **settings.PLUGINS_CONFIG.get("forward_netbox", {}),
        "enable_bgp_sync": True,
    },
}
BGP_DISABLED_PLUGIN_CONFIG = {
    **settings.PLUGINS_CONFIG,
    "forward_netbox": {
        key: value
        for key, value in settings.PLUGINS_CONFIG.get("forward_netbox", {}).items()
        if key != "enable_bgp_sync"
    },
}


class ForwardSourceFormTest(TestCase):
    def _base_form_data(self):
        return {
            "name": "source-1",
            "type": ForwardSourceDeploymentChoices.SAAS,
            "username": "user@example.com",
            "password": "secret",
            "network_id": "test-network",
            "timeout": 1200,
            "nqe_page_size": 10000,
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

    def test_optional_module_model_defaults_unchecked(self):
        form = ForwardSyncForm()

        self.assertFalse(form.fields["dcim.module"].initial)

    @override_settings(PLUGINS_CONFIG=BGP_DISABLED_PLUGIN_CONFIG)
    def test_bgp_models_are_hidden_without_feature_flag(self):
        form = ForwardSyncForm()

        for model_string in FORWARD_BGP_MODELS:
            self.assertNotIn(model_string, form.fields)

    @override_settings(PLUGINS_CONFIG=BGP_PLUGIN_CONFIG)
    def test_bgp_models_are_optional_when_feature_flag_is_enabled(self):
        form = ForwardSyncForm()

        for model_string in FORWARD_BGP_MODELS:
            self.assertIn(model_string, form.fields)
            self.assertFalse(form.fields[model_string].initial)

    def test_form_preserves_auto_merge_and_forces_native_branching(self):
        form = ForwardSyncForm(
            data={
                "name": "sync-1",
                "source": self.source.pk,
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "execution_backend": ForwardExecutionBackendChoices.BRANCHING,
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
        self.assertEqual(
            form.instance.parameters["execution_backend"],
            ForwardExecutionBackendChoices.BRANCHING,
        )

    def test_form_persists_fast_bootstrap_backend(self):
        form = ForwardSyncForm(
            data={
                "name": "sync-fast-bootstrap",
                "source": self.source.pk,
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "execution_backend": ForwardExecutionBackendChoices.FAST_BOOTSTRAP,
                "dcim.device": "on",
                "auto_merge": "on",
                "max_changes_per_branch": "10000",
            }
        )

        self.assertTrue(form.is_valid(), form.errors)
        self.assertEqual(
            form.instance.parameters["execution_backend"],
            ForwardExecutionBackendChoices.FAST_BOOTSTRAP,
        )

    @patch("forward_netbox.forms.ForwardSource.validate_connection")
    def test_source_form_persists_nqe_page_size(self, _mock_validate_connection):
        form = ForwardSourceForm(
            data={
                "name": "source-2",
                "type": ForwardSourceDeploymentChoices.SAAS,
                "username": "user@example.com",
                "password": "secret",
                "network_id": "test-network",
                "timeout": 1200,
                "nqe_page_size": 10000,
                "verify": True,
            }
        )

        self.assertTrue(form.is_valid(), form.errors)
        source = form.save()
        self.assertEqual(source.parameters["nqe_page_size"], 10000)


class ForwardNQEMapFormTest(TestCase):
    def test_repository_path_uses_forward_query_selector(self):
        source = ForwardSource.objects.create(
            name="source-1",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
            },
        )

        form = ForwardNQEMapForm()

        self.assertIn("query_mode", form.fields)
        self.assertIn("query_source", form.fields)
        self.assertIn("query_repository", form.fields)
        self.assertIn("query_folder", form.fields)
        self.assertIn("query_path", form.fields)
        self.assertEqual(
            form.fields["query_folder"].widget.attrs["data-url"],
            "/api/plugins/forward/nqe-map/available-query-folders/",
        )
        self.assertEqual(
            form.fields["query_path"].widget.attrs["data-url"],
            "/api/plugins/forward/nqe-map/available-queries/",
        )
        self.assertEqual(
            form.fields["commit_id"].widget.attrs["data-url"],
            "/api/plugins/forward/nqe-map/available-query-commits/",
        )
        self.assertEqual(form.fields["query_source"].initial, source.pk)
        self.assertEqual(form.fields["query_repository"].initial, "org")
        self.assertEqual(form.fields["query_mode"].initial, "query_path")

    def test_query_path_mode_clears_raw_query_and_direct_id(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="site")
        form = ForwardNQEMapForm(
            data={
                "name": "site-map",
                "netbox_model": netbox_model.pk,
                "query_mode": "query_path",
                "query_repository": "org",
                "query_folder": "/",
                "query_id": "Q_sites",
                "query_path": "/forward_netbox_validation/forward_sites",
                "query": "select {}",
                "commit_id": "commit-1",
                "enabled": True,
                "weight": 100,
            }
        )

        self.assertTrue(form.is_valid(), form.errors)
        nqe_map = form.save(commit=False)
        self.assertEqual(nqe_map.query_id, "")
        self.assertEqual(nqe_map.query_repository, "org")
        self.assertEqual(nqe_map.query_path, "/forward_netbox_validation/forward_sites")
        self.assertEqual(nqe_map.query, "")
        self.assertEqual(nqe_map.commit_id, "commit-1")

    def test_direct_query_id_mode_clears_raw_query_and_path(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="site")
        form = ForwardNQEMapForm(
            data={
                "name": "site-map",
                "netbox_model": netbox_model.pk,
                "query_mode": "query_id",
                "query_repository": "org",
                "query_folder": "/",
                "query_id": "Q_sites",
                "query_path": "/forward_netbox_validation/forward_sites",
                "query": "select {}",
                "commit_id": "commit-1",
                "enabled": True,
                "weight": 100,
            }
        )

        self.assertTrue(form.is_valid(), form.errors)
        nqe_map = form.save(commit=False)
        self.assertEqual(nqe_map.query_id, "Q_sites")
        self.assertEqual(nqe_map.query_path, "")
        self.assertEqual(nqe_map.query, "")
        self.assertEqual(nqe_map.commit_id, "commit-1")

    def test_raw_query_mode_clears_query_id_and_commit(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="site")
        form = ForwardNQEMapForm(
            data={
                "name": "site-map",
                "netbox_model": netbox_model.pk,
                "query_mode": "query",
                "query_repository": "org",
                "query_folder": "/",
                "query_id": "Q_sites",
                "query_path": "/forward_netbox_validation/forward_sites",
                "query": 'select {\n  name: "site-a",\n  slug: "site-a"\n}',
                "commit_id": "commit-1",
                "enabled": True,
                "weight": 100,
            }
        )

        self.assertTrue(form.is_valid(), form.errors)
        nqe_map = form.save(commit=False)
        self.assertEqual(nqe_map.query_id, "")
        self.assertEqual(nqe_map.query_repository, "")
        self.assertEqual(nqe_map.query_path, "")
        self.assertEqual(nqe_map.commit_id, "")
        self.assertIn("site-a", nqe_map.query)

    def test_query_path_mode_requires_query_path(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="site")
        form = ForwardNQEMapForm(
            data={
                "name": "site-map",
                "netbox_model": netbox_model.pk,
                "query_mode": "query_path",
                "query_repository": "org",
                "query_folder": "/",
                "query_id": "",
                "query_path": "",
                "query": "",
                "commit_id": "",
                "enabled": True,
                "weight": 100,
            }
        )

        self.assertFalse(form.is_valid())
        self.assertIn("query_path", form.errors)

    def test_existing_forward_library_query_id_initializes_repository(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="site")
        nqe_map = ForwardNQEMap(
            name="site-map",
            netbox_model=netbox_model,
            query_id="FQ_sites",
        )

        form = ForwardNQEMapForm(instance=nqe_map)

        self.assertEqual(form.fields["query_mode"].initial, "query_id")
        self.assertEqual(form.fields["query_repository"].initial, "fwd")

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


class ForwardNQEMapBulkEditFormTest(TestCase):
    def test_bulk_edit_form_uses_forward_folder_selector(self):
        source = ForwardSource.objects.create(
            name="source-1",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
            },
        )
        netbox_model = ContentType.objects.get(app_label="dcim", model="device")
        query_map = ForwardNQEMap.objects.create(
            name="Custom Device Import",
            netbox_model=netbox_model,
            query='select {name: "device-1"}',
        )

        form = ForwardNQEMapBulkEditForm(initial={"pk": [query_map.pk]})
        query_path_field = ForwardNQEMapBulkEditForm.query_path_field_name(query_map.pk)

        self.assertIn("query_bulk_operation", form.fields)
        self.assertIn("publish_overwrite", form.fields)
        self.assertIn("publish_commit_message", form.fields)
        self.assertEqual(form.fields["bind_query_source"].queryset.count(), 1)
        self.assertEqual(
            form.fields["bind_query_folder"].widget.attrs["data-url"],
            "/api/plugins/forward/nqe-map/available-query-folders/",
        )
        self.assertIn(query_path_field, form.fields)
        self.assertEqual(
            form.fields[query_path_field].widget.attrs["data-url"],
            "/api/plugins/forward/nqe-map/available-queries/",
        )
        dynamic_params = form.fields[query_path_field].widget.attrs[
            "data-dynamic-params"
        ]
        static_params = form.fields[query_path_field].widget.attrs["data-static-params"]
        self.assertIn('"fieldName":"bind_query_folder"', dynamic_params)
        self.assertIn('"queryParam":"directory"', dynamic_params)
        self.assertIn('"queryParam":"model_string"', static_params)
        self.assertIn('"queryValue":["dcim.device"]', static_params)
        self.assertEqual(form.fields["bind_query_repository"].initial, "org")
        self.assertEqual(
            tuple(form.fields["bind_query_repository"].choices),
            FORWARD_NQE_QUERY_REPOSITORY_CHOICES,
        )
        self.assertEqual(source.name, "source-1")

    def test_bulk_edit_bind_requires_explicit_per_map_query_path(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="device")
        query_map = ForwardNQEMap.objects.create(
            name="Custom Device Import",
            netbox_model=netbox_model,
            query='select {name: "device-1"}',
        )
        source = ForwardSource.objects.create(
            name="source-1",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
            },
        )

        form = ForwardNQEMapBulkEditForm(
            data={
                "pk": [query_map.pk],
                "query_bulk_operation": "bind_query_path",
                "bind_query_source": source.pk,
                "bind_query_repository": "org",
                "bind_query_folder": "/forward_netbox_validation/",
            }
        )

        self.assertFalse(form.is_valid())
        self.assertIn("__all__", form.errors)

    def test_bulk_edit_restore_does_not_require_query_lookup_fields(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="device")
        query_map = ForwardNQEMap.objects.create(
            name="Forward Devices",
            netbox_model=netbox_model,
            query_id="Q_devices",
        )

        form = ForwardNQEMapBulkEditForm(
            data={
                "pk": [query_map.pk],
                "query_bulk_operation": "restore_raw_query",
            }
        )

        self.assertTrue(form.is_valid(), form.errors)
        self.assertTrue(form.has_query_restore_request())
        self.assertFalse(form.has_query_binding_request())

    def test_bulk_edit_publish_requires_source_and_folder(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="device")
        query_map = ForwardNQEMap.objects.create(
            name="Forward Devices",
            netbox_model=netbox_model,
            query='select {name: "device-1"}',
        )

        form = ForwardNQEMapBulkEditForm(
            data={
                "pk": [query_map.pk],
                "query_bulk_operation": "publish_bundled_query_path",
                "bind_query_repository": "org",
            }
        )

        self.assertFalse(form.is_valid())
        self.assertIn("bind_query_source", form.errors)
        self.assertIn("bind_query_folder", form.errors)

    def test_bulk_edit_publish_requires_org_repository(self):
        source = ForwardSource.objects.create(
            name="source-1",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
            },
        )
        netbox_model = ContentType.objects.get(app_label="dcim", model="device")
        query_map = ForwardNQEMap.objects.create(
            name="Forward Devices",
            netbox_model=netbox_model,
            query='select {name: "device-1"}',
        )

        form = ForwardNQEMapBulkEditForm(
            data={
                "pk": [query_map.pk],
                "query_bulk_operation": "publish_bundled_query_path",
                "bind_query_source": source.pk,
                "bind_query_repository": "fwd",
                "bind_query_folder": "/forward_netbox_validation/",
            }
        )

        self.assertFalse(form.is_valid())
        self.assertIn("bind_query_repository", form.errors)

    def test_existing_query_path_does_not_trigger_binding_when_unchanged(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="device")
        query_map = ForwardNQEMap.objects.create(
            name="Custom Device Import",
            netbox_model=netbox_model,
            query_repository="org",
            query_path="/forward_netbox_validation/forward_devices",
            query="",
            enabled=True,
        )
        query_path_field = ForwardNQEMapBulkEditForm.query_path_field_name(query_map.pk)

        form = ForwardNQEMapBulkEditForm(
            data={
                "pk": [query_map.pk],
                query_path_field: "/forward_netbox_validation/forward_devices",
                "enabled": "False",
            }
        )

        self.assertTrue(form.is_valid(), form.errors)
        self.assertFalse(form.has_query_binding_request())
        self.assertIn("enabled", form.changed_data)
        self.assertNotIn(query_path_field, form.changed_data)

    def test_bulk_edit_bind_requires_source_and_folder_together(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="device")
        query_map = ForwardNQEMap.objects.create(
            name="Custom Device Import",
            netbox_model=netbox_model,
            query='select {name: "device-1"}',
        )
        form = ForwardNQEMapBulkEditForm(
            data={
                "pk": [query_map.pk],
                "query_bulk_operation": "bind_query_path",
                "bind_query_folder": "/forward_netbox_validation/",
            }
        )

        self.assertFalse(form.is_valid())
        self.assertIn("bind_query_source", form.errors)

    def test_bulk_edit_query_fields_do_not_trigger_binding_without_operation(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="device")
        query_map = ForwardNQEMap.objects.create(
            name="Custom Device Import",
            netbox_model=netbox_model,
            query='select {name: "device-1"}',
        )
        form = ForwardNQEMapBulkEditForm(
            data={
                "pk": [query_map.pk],
                "bind_query_folder": "/forward_netbox_validation/",
            }
        )

        self.assertTrue(form.is_valid(), form.errors)
        self.assertFalse(form.has_query_binding_request())
        self.assertFalse(form.has_query_restore_request())
