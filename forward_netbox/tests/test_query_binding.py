from unittest.mock import Mock
from unittest.mock import patch

from django.contrib.contenttypes.models import ContentType
from django.test import TestCase

from forward_netbox.forms import ForwardNQEMapBulkEditForm
from forward_netbox.models import ForwardNQEMap
from forward_netbox.models import ForwardSource
from forward_netbox.utilities.query_binding import apply_explicit_nqe_map_bindings
from forward_netbox.utilities.query_binding import apply_nqe_map_bindings
from forward_netbox.utilities.query_binding import build_nqe_map_bindings
from forward_netbox.utilities.query_binding import publish_builtin_nqe_map_queries
from forward_netbox.utilities.query_binding import restore_builtin_raw_query_bindings
from forward_netbox.utilities.query_registry import read_builtin_query_source
from forward_netbox.views import ForwardNQEMapBulkEditView


class NQEMapBindingTest(TestCase):
    def test_build_bindings_matches_repository_queries_to_builtin_models(self):
        client = Mock()
        client.get_nqe_repository_queries.return_value = [
            {
                "queryId": "Q_devices",
                "path": "/forward_netbox_validation/forward_devices",
                "lastCommitId": "commit-1",
            },
            {
                "queryId": "Q_unrelated",
                "path": "/forward_netbox_validation/unrelated",
                "lastCommitId": "commit-2",
            },
        ]

        bindings = build_nqe_map_bindings(
            client=client,
            repository="org",
            directory="/forward_netbox_validation/",
            pin_commit=True,
        )

        self.assertEqual(len(bindings), 1)
        self.assertEqual(bindings[0].model_string, "dcim.device")
        self.assertEqual(bindings[0].query_name, "Forward Devices")
        self.assertEqual(bindings[0].query_filename, "forward_devices.nqe")
        self.assertEqual(
            bindings[0].query_path,
            "/forward_netbox_validation/forward_devices",
        )
        self.assertEqual(bindings[0].commit_id, "commit-1")

    def test_apply_bindings_switches_matching_model_to_query_path_mode(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="device")
        query_map = ForwardNQEMap.objects.create(
            name="Custom Device Import",
            netbox_model=netbox_model,
            query_id="Q_old",
            query="",
            commit_id="old-commit",
        )
        bindings = build_nqe_map_bindings(
            client=Mock(
                get_nqe_repository_queries=Mock(
                    return_value=[
                        {
                            "queryId": "Q_devices",
                            "path": "/forward_netbox_validation/forward_devices",
                            "lastCommitId": "commit-1",
                        }
                    ]
                )
            ),
            repository="org",
            directory="/forward_netbox_validation/",
            pin_commit=False,
        )

        results = apply_nqe_map_bindings(
            bindings,
            queryset=ForwardNQEMap.objects.filter(pk=query_map.pk),
        )

        self.assertEqual(len(results), 1)
        self.assertTrue(results[0].matched)
        self.assertEqual(results[0].map_id, query_map.pk)
        query_map.refresh_from_db()
        self.assertEqual(query_map.query_id, "")
        self.assertEqual(query_map.query_repository, "org")
        self.assertEqual(
            query_map.query_path,
            "/forward_netbox_validation/forward_devices",
        )
        self.assertEqual(query_map.query, "")
        self.assertEqual(query_map.commit_id, "")

    def test_bulk_edit_view_binds_selected_maps_through_native_bulk_edit(self):
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
            commit_id="",
        )
        form = ForwardNQEMapBulkEditForm(
            data={
                "pk": [query_map.pk],
                "query_bulk_operation": "bind_query_path",
                "bind_query_source": source.pk,
                "bind_query_repository": "org",
                "bind_query_folder": "/forward_netbox_validation/",
                ForwardNQEMapBulkEditForm.query_path_field_name(
                    query_map.pk
                ): "/forward_netbox_validation/forward_devices",
                "bind_pin_commit": "",
            }
        )
        self.assertTrue(form.is_valid(), form.errors)
        client = Mock()
        client.get_nqe_repository_queries.return_value = [
            {
                "queryId": "Q_devices",
                "path": "/forward_netbox_validation/forward_devices",
                "lastCommitId": "commit-1",
            }
        ]

        view = ForwardNQEMapBulkEditView()
        with patch.object(ForwardSource, "get_client", return_value=client):
            updated = view._update_objects(form, Mock())

        self.assertEqual([obj.pk for obj in updated], [query_map.pk])
        query_map.refresh_from_db()
        self.assertEqual(query_map.query_id, "")
        self.assertEqual(query_map.query_repository, "org")
        self.assertEqual(
            query_map.query_path,
            "/forward_netbox_validation/forward_devices",
        )
        self.assertEqual(query_map.query, "")

    def test_bulk_edit_view_restores_selected_maps_to_raw_query_text(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="device")
        query_map = ForwardNQEMap.objects.create(
            name="Custom Device Import",
            netbox_model=netbox_model,
            query_id="Q_devices",
            query_repository="org",
            query_path="/forward_netbox_validation/forward_devices",
            commit_id="commit-1",
            query="",
        )
        form = ForwardNQEMapBulkEditForm(
            data={
                "pk": [query_map.pk],
                "query_bulk_operation": "restore_raw_query",
            }
        )
        self.assertTrue(form.is_valid(), form.errors)

        updated = ForwardNQEMapBulkEditView()._update_objects(form, Mock())

        self.assertEqual([obj.pk for obj in updated], [query_map.pk])
        query_map.refresh_from_db()
        self.assertEqual(query_map.query_id, "")
        self.assertEqual(query_map.query_repository, "")
        self.assertEqual(query_map.query_path, "")
        self.assertEqual(query_map.commit_id, "")
        self.assertEqual(
            query_map.query, read_builtin_query_source("forward_devices.nqe")
        )

    def test_publish_builtin_queries_adds_sources_commits_and_binds_selected_maps(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="device")
        query_map = ForwardNQEMap.objects.create(
            name="Forward Devices",
            netbox_model=netbox_model,
            query=read_builtin_query_source("forward_devices.nqe"),
        )
        client = Mock()
        client.get_nqe_repository_queries.side_effect = [
            [],
            [
                {
                    "queryId": "OQ_utilities",
                    "path": "/forward_netbox_validation/netbox_utilities",
                    "lastCommitId": "commit-1",
                },
                {
                    "queryId": "OQ_devices",
                    "path": "/forward_netbox_validation/forward_devices",
                    "lastCommitId": "commit-1",
                },
            ],
        ]
        client.commit_org_nqe_queries.return_value = "commit-1"

        results = publish_builtin_nqe_map_queries(
            client=client,
            directory="/forward_netbox_validation/",
            queryset=ForwardNQEMap.objects.filter(pk=query_map.pk),
            commit_message="Publish Forward NetBox NQE maps",
            pin_commit=True,
        )

        published_paths = [
            call.kwargs["query_path"]
            for call in client.add_org_nqe_query.call_args_list
        ]
        self.assertEqual(
            published_paths,
            [
                "/forward_netbox_validation/netbox_utilities",
                "/forward_netbox_validation/forward_devices",
            ],
        )
        client.commit_org_nqe_queries.assert_called_once_with(
            query_paths=published_paths,
            message="Publish Forward NetBox NQE maps",
        )
        self.assertTrue(any(result.matched for result in results))
        query_map.refresh_from_db()
        self.assertEqual(query_map.query_repository, "org")
        self.assertEqual(
            query_map.query_path,
            "/forward_netbox_validation/forward_devices",
        )
        self.assertEqual(query_map.query, "")
        self.assertEqual(query_map.commit_id, "commit-1")

    def test_bulk_edit_view_publishes_and_binds_selected_maps(self):
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
            query=read_builtin_query_source("forward_devices.nqe"),
        )
        form = ForwardNQEMapBulkEditForm(
            data={
                "pk": [query_map.pk],
                "query_bulk_operation": "publish_bundled_query_path",
                "bind_query_source": source.pk,
                "bind_query_repository": "org",
                "bind_query_folder": "/forward_netbox_validation/",
                "publish_commit_message": "Publish Forward NetBox NQE maps",
            }
        )
        self.assertTrue(form.is_valid(), form.errors)
        client = Mock()
        client.get_nqe_repository_queries.side_effect = [
            [],
            [
                {
                    "queryId": "OQ_utilities",
                    "path": "/forward_netbox_validation/netbox_utilities",
                    "lastCommitId": "commit-1",
                },
                {
                    "queryId": "OQ_devices",
                    "path": "/forward_netbox_validation/forward_devices",
                    "lastCommitId": "commit-1",
                },
            ],
        ]
        client.commit_org_nqe_queries.return_value = "commit-1"

        view = ForwardNQEMapBulkEditView()
        with patch.object(ForwardSource, "get_client", return_value=client):
            updated = view._update_objects(form, Mock())

        self.assertEqual([obj.pk for obj in updated], [query_map.pk])
        query_map.refresh_from_db()
        self.assertEqual(
            query_map.query_path,
            "/forward_netbox_validation/forward_devices",
        )

    def test_apply_bindings_skips_ambiguous_same_model_maps(self):
        netbox_model = ContentType.objects.get(app_label="ipam", model="prefix")
        query_map = ForwardNQEMap.objects.create(
            name="Custom Prefix Import",
            netbox_model=netbox_model,
            query='select {prefix: "10.0.0.0/24"}',
        )
        bindings = build_nqe_map_bindings(
            client=Mock(
                get_nqe_repository_queries=Mock(
                    return_value=[
                        {
                            "queryId": "Q_ipv4",
                            "path": "/forward_netbox_validation/forward_prefixes_ipv4",
                        },
                        {
                            "queryId": "Q_ipv6",
                            "path": "/forward_netbox_validation/forward_prefixes_ipv6",
                        },
                    ]
                )
            ),
            repository="org",
            directory="/forward_netbox_validation/",
        )

        results = apply_nqe_map_bindings(
            bindings,
            queryset=ForwardNQEMap.objects.filter(pk=query_map.pk),
        )

        self.assertEqual(len(results), 1)
        self.assertFalse(results[0].matched)
        self.assertIn("Multiple repository queries", results[0].skipped_reason)
        query_map.refresh_from_db()
        self.assertEqual(query_map.query_path, "")
        self.assertEqual(query_map.query, 'select {prefix: "10.0.0.0/24"}')

    def test_restore_raw_query_disambiguates_same_model_maps_by_query_path(self):
        netbox_model = ContentType.objects.get(app_label="ipam", model="prefix")
        query_map = ForwardNQEMap.objects.create(
            name="Custom Prefix Import",
            netbox_model=netbox_model,
            query_repository="org",
            query_path="/forward_netbox_validation/forward_prefixes_ipv4",
            query="",
        )

        results = restore_builtin_raw_query_bindings(
            queryset=ForwardNQEMap.objects.filter(pk=query_map.pk)
        )

        self.assertEqual(len(results), 1)
        self.assertTrue(results[0].matched)
        self.assertEqual(results[0].query_filename, "forward_prefixes_ipv4.nqe")
        query_map.refresh_from_db()
        self.assertEqual(query_map.query_path, "")
        self.assertEqual(
            query_map.query,
            read_builtin_query_source("forward_prefixes_ipv4.nqe"),
        )

    def test_restore_raw_query_skips_ambiguous_same_model_custom_maps(self):
        netbox_model = ContentType.objects.get(app_label="ipam", model="prefix")
        query_map = ForwardNQEMap.objects.create(
            name="Custom Prefix Import",
            netbox_model=netbox_model,
            query='select {prefix: "10.0.0.0/24"}',
        )

        results = restore_builtin_raw_query_bindings(
            queryset=ForwardNQEMap.objects.filter(pk=query_map.pk)
        )

        self.assertEqual(len(results), 1)
        self.assertFalse(results[0].matched)
        self.assertIn("Multiple bundled queries", results[0].skipped_reason)
        query_map.refresh_from_db()
        self.assertEqual(query_map.query, 'select {prefix: "10.0.0.0/24"}')

    def test_apply_bindings_disambiguates_same_model_maps_by_raw_query_source(self):
        netbox_model = ContentType.objects.get(app_label="ipam", model="prefix")
        query_map = ForwardNQEMap.objects.create(
            name="Custom IPv4 Prefix Import",
            netbox_model=netbox_model,
            query=read_builtin_query_source("forward_prefixes_ipv4.nqe"),
        )
        bindings = build_nqe_map_bindings(
            client=Mock(
                get_nqe_repository_queries=Mock(
                    return_value=[
                        {
                            "queryId": "Q_ipv4",
                            "path": "/forward_netbox_validation/forward_prefixes_ipv4",
                        },
                        {
                            "queryId": "Q_ipv6",
                            "path": "/forward_netbox_validation/forward_prefixes_ipv6",
                        },
                    ]
                )
            ),
            repository="org",
            directory="/forward_netbox_validation/",
        )

        results = apply_nqe_map_bindings(
            bindings,
            queryset=ForwardNQEMap.objects.filter(pk=query_map.pk),
        )

        self.assertEqual(len(results), 1)
        self.assertTrue(results[0].matched)
        self.assertEqual(results[0].query_filename, "forward_prefixes_ipv4.nqe")
        query_map.refresh_from_db()
        self.assertEqual(
            query_map.query_path,
            "/forward_netbox_validation/forward_prefixes_ipv4",
        )

    def test_apply_explicit_bindings_rejects_wrong_model_selection(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="device")
        query_map = ForwardNQEMap.objects.create(
            name="Custom Device Import",
            netbox_model=netbox_model,
            query='select {name: "device-1"}',
        )
        bindings = build_nqe_map_bindings(
            client=Mock(
                get_nqe_repository_queries=Mock(
                    return_value=[
                        {
                            "queryId": "Q_interfaces",
                            "path": "/forward_netbox_validation/forward_interfaces",
                        }
                    ]
                )
            ),
            repository="org",
            directory="/forward_netbox_validation/",
        )

        results = apply_explicit_nqe_map_bindings(
            bindings,
            query_path_by_map_id={
                query_map.pk: "/forward_netbox_validation/forward_interfaces"
            },
            queryset=ForwardNQEMap.objects.filter(pk=query_map.pk),
        )

        self.assertEqual(len(results), 1)
        self.assertFalse(results[0].matched)
        self.assertIn("not dcim.device", results[0].skipped_reason)
        query_map.refresh_from_db()
        self.assertEqual(query_map.query_path, "")
