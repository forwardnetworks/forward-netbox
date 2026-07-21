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
from forward_netbox.utilities.query_binding import live_query_binding_drift
from forward_netbox.utilities.query_binding import local_query_binding_drift
from forward_netbox.utilities.query_binding import publish_builtin_nqe_map_queries
from forward_netbox.utilities.query_binding import restore_builtin_raw_query_bindings
from forward_netbox.utilities.query_registry import read_builtin_query_source
from forward_netbox.utilities.query_registry import read_compiled_builtin_query_source
from forward_netbox.views import ForwardNQEMapBulkEditView


class NQEMapBindingTest(TestCase):
    def test_build_bindings_matches_repository_queries_to_builtin_models(self):
        client = Mock()
        client.get_nqe_repository_query_index.return_value = {
            "rows": [
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
            ],
            "by_path": {
                "/forward_netbox_validation/forward_devices": {
                    "queryId": "Q_devices",
                    "path": "/forward_netbox_validation/forward_devices",
                    "lastCommitId": "commit-1",
                },
                "/forward_netbox_validation/unrelated": {
                    "queryId": "Q_unrelated",
                    "path": "/forward_netbox_validation/unrelated",
                    "lastCommitId": "commit-2",
                },
            },
            "by_query_id": {
                "Q_devices": [
                    {
                        "queryId": "Q_devices",
                        "path": "/forward_netbox_validation/forward_devices",
                        "lastCommitId": "commit-1",
                    }
                ],
                "Q_unrelated": [
                    {
                        "queryId": "Q_unrelated",
                        "path": "/forward_netbox_validation/unrelated",
                        "lastCommitId": "commit-2",
                    }
                ],
            },
        }

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

    def test_build_bindings_uses_provided_query_index_without_refetching(self):
        client = Mock()
        query_index = {
            "rows": [
                {
                    "queryId": "Q_devices",
                    "path": "/forward_netbox_validation/forward_devices",
                    "lastCommitId": "commit-1",
                }
            ]
        }

        bindings = build_nqe_map_bindings(
            client=client,
            repository="org",
            directory="/forward_netbox_validation/",
            query_index=query_index,
        )

        self.assertEqual(len(bindings), 1)
        self.assertEqual(bindings[0].query_id, "Q_devices")
        client.get_nqe_repository_query_index.assert_not_called()

    def test_apply_bindings_switches_matching_model_to_repository_path(self):
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
                get_nqe_repository_query_index=Mock(
                    return_value={
                        "rows": [
                            {
                                "queryId": "Q_devices",
                                "path": "/forward_netbox_validation/forward_devices",
                                "lastCommitId": "commit-1",
                            }
                        ],
                        "by_path": {
                            "/forward_netbox_validation/forward_devices": {
                                "queryId": "Q_devices",
                                "path": "/forward_netbox_validation/forward_devices",
                                "lastCommitId": "commit-1",
                            }
                        },
                        "by_query_id": {
                            "Q_devices": [
                                {
                                    "queryId": "Q_devices",
                                    "path": "/forward_netbox_validation/forward_devices",
                                    "lastCommitId": "commit-1",
                                }
                            ]
                        },
                    }
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
        self.assertEqual(query_map.commit_id, "old-commit")

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
            commit_id="old-commit",
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
        client.get_nqe_repository_query_index.return_value = {
            "rows": [
                {
                    "queryId": "Q_devices",
                    "path": "/forward_netbox_validation/forward_devices",
                    "lastCommitId": "commit-1",
                }
            ],
            "by_path": {
                "/forward_netbox_validation/forward_devices": {
                    "queryId": "Q_devices",
                    "path": "/forward_netbox_validation/forward_devices",
                    "lastCommitId": "commit-1",
                }
            },
            "by_query_id": {
                "Q_devices": [
                    {
                        "queryId": "Q_devices",
                        "path": "/forward_netbox_validation/forward_devices",
                        "lastCommitId": "commit-1",
                    }
                ]
            },
        }

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
        self.assertEqual(query_map.commit_id, "")

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

    def test_local_query_binding_drift_reports_raw_modified_query(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="device")
        query_map = ForwardNQEMap.objects.create(
            name="Forward Devices",
            netbox_model=netbox_model,
            query='foreach device in network.devices select {name: "changed"}',
        )

        drift = local_query_binding_drift(query_map)

        self.assertEqual(drift["status"], "bundled_raw_modified")
        self.assertEqual(drift["severity"], "warn")
        self.assertEqual(drift["expected_filename"], "forward_devices.nqe")
        self.assertEqual(drift["commit_binding"], "raw_query_not_applicable")

    def test_local_query_binding_drift_reports_repository_path_match(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="device")
        query_map = ForwardNQEMap.objects.create(
            name="Forward Devices",
            netbox_model=netbox_model,
            query_repository="org",
            query_path="/forward_netbox_validation/forward_devices",
        )

        drift = local_query_binding_drift(query_map)

        self.assertEqual(
            drift["status"],
            "repository_path_matches_bundled_filename",
        )
        self.assertEqual(drift["severity"], "pass")
        self.assertEqual(drift["commit_binding"], "latest_commit")
        self.assertIn(
            "latest committed Forward query revision", drift["commit_message"]
        )

    def test_local_query_binding_drift_reports_pinned_commit_guidance(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="device")
        query_map = ForwardNQEMap.objects.create(
            name="Forward Devices",
            netbox_model=netbox_model,
            query_repository="org",
            query_path="/forward_netbox_validation/forward_devices",
            commit_id="commit-1",
        )

        drift = local_query_binding_drift(query_map)

        self.assertEqual(drift["status"], "repository_path_matches_bundled_filename")
        self.assertEqual(drift["commit_binding"], "pinned_commit")
        self.assertIn("pinned to a Forward query commit", drift["commit_message"])

    def test_local_query_binding_drift_reports_direct_query_id_unverified(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="device")
        query_map = ForwardNQEMap.objects.create(
            name="Forward Devices",
            netbox_model=netbox_model,
            query_id="query-devices",
        )

        drift = local_query_binding_drift(query_map)

        self.assertEqual(drift["status"], "direct_query_id_unverified")
        self.assertEqual(drift["severity"], "info")
        self.assertEqual(drift["commit_binding"], "latest_commit")
        self.assertIn("Publish Bundled Queries", drift["remediation"])
        self.assertEqual(drift["remediation_action"], "publish_bundled_queries")

    def test_live_query_binding_drift_reports_repository_source_match(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="device")
        query_map = ForwardNQEMap.objects.create(
            name="Forward Devices",
            netbox_model=netbox_model,
            query_repository="org",
            query_path="/forward_netbox_validation/forward_devices",
        )
        client = Mock()
        client.get_committed_nqe_query.return_value = {
            "queryId": "Q_devices",
            "lastCommitId": "commit-1",
            "path": "/forward_netbox_validation/forward_devices",
            "sourceCode": read_compiled_builtin_query_source("forward_devices.nqe"),
        }

        drift = live_query_binding_drift(client=client, query_map=query_map)

        self.assertEqual(drift["status"], "live_repository_source_match")
        self.assertEqual(drift["severity"], "pass")
        self.assertTrue(drift["live_checked"])
        self.assertEqual(drift["live_query_id"], "Q_devices")
        self.assertEqual(drift["live_commit_id"], "commit-1")
        self.assertEqual(drift["requested_commit_id"], "head")

    def test_live_query_binding_drift_reports_repository_source_modified(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="device")
        query_map = ForwardNQEMap.objects.create(
            name="Forward Devices",
            netbox_model=netbox_model,
            query_repository="org",
            query_path="/forward_netbox_validation/forward_devices",
        )
        client = Mock()
        client.get_committed_nqe_query.return_value = {
            "queryId": "Q_devices",
            "lastCommitId": "commit-1",
            "path": "/forward_netbox_validation/forward_devices",
            "sourceCode": 'foreach device in network.devices select {name: "changed"}',
        }

        drift = live_query_binding_drift(client=client, query_map=query_map)

        self.assertEqual(drift["status"], "live_repository_source_modified")
        self.assertEqual(drift["severity"], "warn")
        self.assertFalse(drift["source_matches_bundled"])

    def test_live_query_binding_drift_resolves_direct_query_id_to_repository(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="device")
        query_map = ForwardNQEMap.objects.create(
            name="Forward Devices",
            netbox_model=netbox_model,
            query_id="Q_devices",
        )
        client = Mock()
        client.get_nqe_repository_query_index.side_effect = [
            {
                "by_query_id": {
                    "Q_devices": [
                        {
                            "queryId": "Q_devices",
                            "path": "/forward_netbox_validation/forward_devices",
                            "lastCommitId": "commit-1",
                        }
                    ]
                }
            },
            {"by_query_id": {}},
        ]
        client.get_committed_nqe_query.return_value = {
            "queryId": "Q_devices",
            "lastCommitId": "commit-1",
            "path": "/forward_netbox_validation/forward_devices",
            "sourceCode": read_compiled_builtin_query_source("forward_devices.nqe"),
        }

        drift = live_query_binding_drift(client=client, query_map=query_map)

        self.assertEqual(drift["status"], "live_repository_source_match")
        self.assertEqual(drift["severity"], "pass")
        self.assertEqual(drift["live_repository"], "org")

    def test_live_query_binding_drift_uses_pinned_direct_query_commit(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="device")
        query_map = ForwardNQEMap.objects.create(
            name="Forward Devices",
            netbox_model=netbox_model,
            query_id="Q_devices",
            commit_id="commit-pinned",
        )
        client = Mock()
        client.get_nqe_repository_query_index.side_effect = [
            {
                "by_query_id": {
                    "Q_devices": [
                        {
                            "queryId": "Q_devices",
                            "path": "/forward_netbox_validation/forward_devices",
                            "lastCommitId": "commit-latest",
                        }
                    ]
                }
            },
            {"by_query_id": {}},
        ]
        client.get_committed_nqe_query.return_value = {
            "queryId": "Q_devices",
            "lastCommitId": "commit-pinned",
            "path": "/forward_netbox_validation/forward_devices",
            "sourceCode": read_compiled_builtin_query_source("forward_devices.nqe"),
        }

        drift = live_query_binding_drift(client=client, query_map=query_map)

        self.assertEqual(drift["status"], "live_repository_source_match")
        self.assertEqual(drift["commit_binding"], "pinned_commit")
        self.assertEqual(drift["requested_commit_id"], "commit-pinned")
        client.get_committed_nqe_query.assert_called_once_with(
            repository="org",
            query_path="/forward_netbox_validation/forward_devices",
            commit_id="commit-pinned",
            require_source_code=True,
        )

    def test_live_query_binding_drift_warns_when_direct_query_id_not_found(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="device")
        query_map = ForwardNQEMap.objects.create(
            name="Forward Devices",
            netbox_model=netbox_model,
            query_id="Q_missing",
        )
        client = Mock()
        client.get_nqe_repository_query_index.return_value = {"by_query_id": {}}

        drift = live_query_binding_drift(client=client, query_map=query_map)

        self.assertEqual(drift["status"], "direct_query_id_unverified")
        self.assertEqual(drift["severity"], "warn")
        self.assertEqual(drift["live_status"], "direct_query_id_not_found")
        self.assertIn("Publish Bundled Queries", drift["remediation"])
        self.assertEqual(drift["remediation_action"], "publish_bundled_queries")

    def test_publish_builtin_queries_adds_sources_commits_and_binds_selected_maps(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="device")
        query_map = ForwardNQEMap.objects.create(
            name="Forward Devices",
            netbox_model=netbox_model,
            query=read_builtin_query_source("forward_devices.nqe"),
        )
        client = Mock()
        client.get_nqe_repository_query_index.side_effect = [
            {"rows": [], "by_path": {}, "by_query_id": {}},
            {
                "rows": [
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
                "by_path": {
                    "/forward_netbox_validation/netbox_utilities": {
                        "queryId": "OQ_utilities",
                        "path": "/forward_netbox_validation/netbox_utilities",
                        "lastCommitId": "commit-1",
                    },
                    "/forward_netbox_validation/forward_devices": {
                        "queryId": "OQ_devices",
                        "path": "/forward_netbox_validation/forward_devices",
                        "lastCommitId": "commit-1",
                    },
                },
                "by_query_id": {
                    "OQ_utilities": [
                        {
                            "queryId": "OQ_utilities",
                            "path": "/forward_netbox_validation/netbox_utilities",
                            "lastCommitId": "commit-1",
                        }
                    ],
                    "OQ_devices": [
                        {
                            "queryId": "OQ_devices",
                            "path": "/forward_netbox_validation/forward_devices",
                            "lastCommitId": "commit-1",
                        }
                    ],
                },
            },
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
                "/forward_netbox_validation/forward_devices",
            ],
        )
        published_devices = client.add_org_nqe_query.call_args_list[0]
        self.assertEqual(
            published_devices.kwargs["source_code"],
            read_compiled_builtin_query_source("forward_devices.nqe"),
        )
        self.assertNotIn(
            'import "netbox_utilities";',
            published_devices.kwargs["source_code"],
        )
        client.commit_org_nqe_queries.assert_called_once_with(
            query_paths=published_paths,
            message="Publish Forward NetBox NQE maps",
        )
        self.assertEqual(client.get_nqe_repository_query_index.call_count, 2)
        client.get_nqe_repository_query_index.assert_any_call(
            repository="org",
            directory="/forward_netbox_validation/",
        )
        self.assertTrue(any(result.matched for result in results))
        query_map.refresh_from_db()
        self.assertEqual(query_map.query_repository, "org")
        self.assertEqual(query_map.query_id, "")
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
        client.get_nqe_repository_query_index.side_effect = [
            {
                "rows": [],
                "by_path": {},
                "by_query_id": {},
            },
            {
                "rows": [
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
                "by_path": {
                    "/forward_netbox_validation/netbox_utilities": {
                        "queryId": "OQ_utilities",
                        "path": "/forward_netbox_validation/netbox_utilities",
                        "lastCommitId": "commit-1",
                    },
                    "/forward_netbox_validation/forward_devices": {
                        "queryId": "OQ_devices",
                        "path": "/forward_netbox_validation/forward_devices",
                        "lastCommitId": "commit-1",
                    },
                },
                "by_query_id": {
                    "OQ_utilities": [
                        {
                            "queryId": "OQ_utilities",
                            "path": "/forward_netbox_validation/netbox_utilities",
                            "lastCommitId": "commit-1",
                        }
                    ],
                    "OQ_devices": [
                        {
                            "queryId": "OQ_devices",
                            "path": "/forward_netbox_validation/forward_devices",
                            "lastCommitId": "commit-1",
                        }
                    ],
                },
            },
        ]
        client.commit_org_nqe_queries.return_value = "commit-1"

        view = ForwardNQEMapBulkEditView()
        with patch.object(ForwardSource, "get_client", return_value=client):
            updated = view._update_objects(form, Mock())

        self.assertEqual([obj.pk for obj in updated], [query_map.pk])
        query_map.refresh_from_db()
        self.assertEqual(query_map.query_id, "")
        self.assertEqual(
            query_map.query_path,
            "/forward_netbox_validation/forward_devices",
        )
        self.assertEqual(query_map.commit_id, "")
        self.assertEqual(client.get_nqe_repository_query_index.call_count, 2)
        client.get_nqe_repository_query_index.assert_any_call(
            repository="org",
            directory="/forward_netbox_validation/",
        )

    def test_publish_builtin_queries_reuses_initial_query_index_when_no_changes(
        self,
    ):
        netbox_model = ContentType.objects.get(app_label="dcim", model="device")
        query_map = ForwardNQEMap.objects.create(
            name="Forward Devices",
            netbox_model=netbox_model,
            query=read_builtin_query_source("forward_devices.nqe"),
            query_repository="org",
            query_path="/forward_netbox_validation/forward_devices",
        )
        client = Mock()
        client.get_nqe_repository_query_index.return_value = {
            "rows": [
                {
                    "queryId": "OQ_devices",
                    "path": "/forward_netbox_validation/forward_devices",
                    "lastCommitId": "commit-1",
                }
            ],
            "by_path": {
                "/forward_netbox_validation/forward_devices": {
                    "queryId": "OQ_devices",
                    "path": "/forward_netbox_validation/forward_devices",
                    "lastCommitId": "commit-1",
                }
            },
            "by_query_id": {
                "OQ_devices": [
                    {
                        "queryId": "OQ_devices",
                        "path": "/forward_netbox_validation/forward_devices",
                        "lastCommitId": "commit-1",
                    }
                ]
            },
        }

        results = publish_builtin_nqe_map_queries(
            client=client,
            directory="/forward_netbox_validation/",
            queryset=ForwardNQEMap.objects.filter(pk=query_map.pk),
            commit_message="Publish Forward NetBox NQE maps",
            pin_commit=False,
        )

        self.assertEqual(len(results), 1)
        self.assertTrue(results[0].matched)
        query_map.refresh_from_db()
        self.assertEqual(query_map.query_id, "")
        self.assertEqual(
            query_map.query_path,
            "/forward_netbox_validation/forward_devices",
        )
        self.assertEqual(query_map.commit_id, "")
        client.get_nqe_repository_query_index.assert_called_once_with(
            repository="org",
            directory="/forward_netbox_validation/",
        )

    def test_publish_builtin_queries_preserves_existing_commit_pin(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="device")
        query_map = ForwardNQEMap.objects.create(
            name="Forward Devices",
            netbox_model=netbox_model,
            query_id="Q_devices_old",
            commit_id="commit-pinned",
        )
        client = Mock()
        client.get_nqe_repository_query_index.return_value = {
            "rows": [
                {
                    "queryId": "Q_devices_head",
                    "path": "/forward_netbox_validation/forward_devices",
                    "lastCommitId": "commit-head",
                }
            ],
            "by_path": {
                "/forward_netbox_validation/forward_devices": {
                    "queryId": "Q_devices_head",
                    "path": "/forward_netbox_validation/forward_devices",
                    "lastCommitId": "commit-head",
                }
            },
            "by_query_id": {},
        }

        results = publish_builtin_nqe_map_queries(
            client=client,
            directory="/forward_netbox_validation/",
            queryset=ForwardNQEMap.objects.filter(pk=query_map.pk),
            commit_message="Publish Forward NetBox NQE maps",
            pin_commit=False,
        )

        self.assertEqual(len(results), 1)
        self.assertTrue(results[0].matched)
        query_map.refresh_from_db()
        self.assertEqual(query_map.query_id, "")
        self.assertEqual(
            query_map.query_path,
            "/forward_netbox_validation/forward_devices",
        )
        self.assertEqual(query_map.commit_id, "commit-pinned")

    def test_publish_builtin_queries_explicit_repin_replaces_existing_pin(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="device")
        query_map = ForwardNQEMap.objects.create(
            name="Forward Devices",
            netbox_model=netbox_model,
            query_id="Q_devices_old",
            commit_id="commit-pinned",
        )
        client = Mock()
        client.get_nqe_repository_query_index.return_value = {
            "rows": [
                {
                    "queryId": "Q_devices_head",
                    "path": "/forward_netbox_validation/forward_devices",
                    "lastCommitId": "commit-head",
                }
            ],
            "by_path": {
                "/forward_netbox_validation/forward_devices": {
                    "queryId": "Q_devices_head",
                    "path": "/forward_netbox_validation/forward_devices",
                    "lastCommitId": "commit-head",
                }
            },
            "by_query_id": {},
        }

        results = publish_builtin_nqe_map_queries(
            client=client,
            directory="/forward_netbox_validation/",
            queryset=ForwardNQEMap.objects.filter(pk=query_map.pk),
            commit_message="Repin Forward NetBox NQE maps",
            pin_commit=True,
        )

        self.assertEqual(len(results), 1)
        self.assertTrue(results[0].matched)
        query_map.refresh_from_db()
        self.assertEqual(query_map.query_id, "")
        self.assertEqual(
            query_map.query_path,
            "/forward_netbox_validation/forward_devices",
        )
        self.assertEqual(query_map.commit_id, "commit-head")

    def test_publish_builtin_queries_resolves_commit_id_for_existing_path(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="device")
        query_map = ForwardNQEMap.objects.create(
            name="Forward Devices",
            netbox_model=netbox_model,
            query=read_builtin_query_source("forward_devices.nqe"),
            query_repository="org",
            query_path="/forward_netbox_validation/forward_devices",
        )
        client = Mock()
        client.get_nqe_repository_query_index.return_value = {
            "rows": [
                {
                    "queryId": "OQ_devices",
                    "path": "/forward_netbox_validation/forward_devices",
                }
            ],
            "by_path": {
                "/forward_netbox_validation/forward_devices": {
                    "queryId": "OQ_devices",
                    "path": "/forward_netbox_validation/forward_devices",
                }
            },
            "by_query_id": {
                "OQ_devices": [
                    {
                        "queryId": "OQ_devices",
                        "path": "/forward_netbox_validation/forward_devices",
                    }
                ]
            },
        }
        client.get_nqe_query_history.return_value = [
            {
                "id": "commit-old",
                "path": "/forward_netbox_validation/forward_devices",
            },
            {
                "id": "commit-1",
                "path": "/forward_netbox_validation/forward_devices",
            },
        ]
        client.commit_org_nqe_queries.return_value = "commit-1"

        results = publish_builtin_nqe_map_queries(
            client=client,
            directory="/forward_netbox_validation/",
            queryset=ForwardNQEMap.objects.filter(pk=query_map.pk),
            commit_message="Publish Forward NetBox NQE maps",
            pin_commit=True,
            overwrite=True,
        )

        client.edit_org_nqe_query.assert_called_once_with(
            query_path="/forward_netbox_validation/forward_devices",
            source_code=read_compiled_builtin_query_source("forward_devices.nqe"),
            query_id="OQ_devices",
            commit_id="commit-1",
        )
        self.assertTrue(any(result.matched for result in results))
        query_map.refresh_from_db()
        self.assertEqual(query_map.query_id, "")
        self.assertEqual(
            query_map.query_path,
            "/forward_netbox_validation/forward_devices",
        )
        self.assertEqual(query_map.commit_id, "commit-1")

    def test_publish_builtin_queries_skips_edit_when_overwrite_source_unchanged(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="device")
        compiled_source = read_compiled_builtin_query_source("forward_devices.nqe")
        query_map = ForwardNQEMap.objects.create(
            name="Forward Devices",
            netbox_model=netbox_model,
            query=read_builtin_query_source("forward_devices.nqe"),
            query_repository="org",
            query_path="/forward_netbox_validation/forward_devices",
        )
        client = Mock()
        client.get_nqe_repository_query_index.return_value = {
            "rows": [
                {
                    "queryId": "OQ_devices",
                    "path": "/forward_netbox_validation/forward_devices",
                }
            ],
            "by_path": {
                "/forward_netbox_validation/forward_devices": {
                    "queryId": "OQ_devices",
                    "path": "/forward_netbox_validation/forward_devices",
                }
            },
            "by_query_id": {
                "OQ_devices": [
                    {
                        "queryId": "OQ_devices",
                        "path": "/forward_netbox_validation/forward_devices",
                    }
                ]
            },
        }
        # _committed_query_by_path falls back to get_nqe_query_history when the
        # map record has no commit_id; return empty so it proceeds to
        # get_committed_nqe_query where the source comparison is made.
        client.get_nqe_query_history.return_value = []
        # Committed source matches bundled compiled source — no edit needed.
        client.get_committed_nqe_query.return_value = {
            "queryId": "OQ_devices",
            "query": compiled_source,
            "lastCommitId": "commit-1",
            "path": "/forward_netbox_validation/forward_devices",
        }

        publish_builtin_nqe_map_queries(
            client=client,
            directory="/forward_netbox_validation/",
            queryset=ForwardNQEMap.objects.filter(pk=query_map.pk),
            commit_message="Publish",
            pin_commit=False,
            overwrite=True,
        )

        client.edit_org_nqe_query.assert_not_called()
        client.commit_org_nqe_queries.assert_not_called()

    def test_apply_bindings_uses_map_name_to_resolve_same_model_candidates(self):
        netbox_model = ContentType.objects.get(app_label="dcim", model="device")
        query_map = ForwardNQEMap.objects.create(
            name="Forward Devices with NetBox Device Type Aliases",
            netbox_model=netbox_model,
            query_id="Q_old_root_alias",
            commit_id="old-commit",
        )
        client = Mock()
        client.get_nqe_repository_query_index.return_value = {
            "rows": [
                {
                    "queryId": "Q_devices",
                    "path": "/forward_netbox_validation/forward_devices",
                },
                {
                    "queryId": "Q_alias",
                    "path": "/forward_netbox_validation/forward_devices_with_netbox_aliases",
                },
            ],
            "by_path": {
                "/forward_netbox_validation/forward_devices": {
                    "queryId": "Q_devices",
                    "path": "/forward_netbox_validation/forward_devices",
                },
                "/forward_netbox_validation/forward_devices_with_netbox_aliases": {
                    "queryId": "Q_alias",
                    "path": "/forward_netbox_validation/forward_devices_with_netbox_aliases",
                },
            },
            "by_query_id": {
                "Q_devices": [
                    {
                        "queryId": "Q_devices",
                        "path": "/forward_netbox_validation/forward_devices",
                    }
                ],
                "Q_alias": [
                    {
                        "queryId": "Q_alias",
                        "path": "/forward_netbox_validation/forward_devices_with_netbox_aliases",
                    }
                ],
            },
        }

        bindings = build_nqe_map_bindings(
            client=client,
            repository="org",
            directory="/forward_netbox_validation/",
        )
        results = apply_nqe_map_bindings(
            bindings,
            queryset=ForwardNQEMap.objects.filter(pk=query_map.pk),
        )

        self.assertEqual(len(results), 1)
        self.assertTrue(results[0].matched)
        self.assertEqual(
            results[0].query_path,
            "/forward_netbox_validation/forward_devices_with_netbox_aliases",
        )
        query_map.refresh_from_db()
        self.assertEqual(query_map.query_id, "")
        self.assertEqual(
            query_map.query_path,
            "/forward_netbox_validation/forward_devices_with_netbox_aliases",
        )
        self.assertEqual(query_map.commit_id, "old-commit")

    def test_apply_bindings_skips_ambiguous_same_model_maps(self):
        netbox_model = ContentType.objects.get(app_label="ipam", model="prefix")
        query_map = ForwardNQEMap.objects.create(
            name="Custom Prefix Import",
            netbox_model=netbox_model,
            query='select {prefix: "10.0.0.0/24"}',
        )
        bindings = build_nqe_map_bindings(
            client=Mock(
                get_nqe_repository_query_index=Mock(
                    return_value={
                        "rows": [
                            {
                                "queryId": "Q_ipv4",
                                "path": "/forward_netbox_validation/forward_prefixes_ipv4",
                            },
                            {
                                "queryId": "Q_ipv6",
                                "path": "/forward_netbox_validation/forward_prefixes_ipv6",
                            },
                        ],
                        "by_path": {
                            "/forward_netbox_validation/forward_prefixes_ipv4": {
                                "queryId": "Q_ipv4",
                                "path": "/forward_netbox_validation/forward_prefixes_ipv4",
                            },
                            "/forward_netbox_validation/forward_prefixes_ipv6": {
                                "queryId": "Q_ipv6",
                                "path": "/forward_netbox_validation/forward_prefixes_ipv6",
                            },
                        },
                        "by_query_id": {
                            "Q_ipv4": [
                                {
                                    "queryId": "Q_ipv4",
                                    "path": "/forward_netbox_validation/forward_prefixes_ipv4",
                                }
                            ],
                            "Q_ipv6": [
                                {
                                    "queryId": "Q_ipv6",
                                    "path": "/forward_netbox_validation/forward_prefixes_ipv6",
                                }
                            ],
                        },
                    }
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
                get_nqe_repository_query_index=Mock(
                    return_value={
                        "rows": [
                            {
                                "queryId": "Q_ipv4",
                                "path": "/forward_netbox_validation/forward_prefixes_ipv4",
                            },
                            {
                                "queryId": "Q_ipv6",
                                "path": "/forward_netbox_validation/forward_prefixes_ipv6",
                            },
                        ],
                        "by_path": {
                            "/forward_netbox_validation/forward_prefixes_ipv4": {
                                "queryId": "Q_ipv4",
                                "path": "/forward_netbox_validation/forward_prefixes_ipv4",
                            },
                            "/forward_netbox_validation/forward_prefixes_ipv6": {
                                "queryId": "Q_ipv6",
                                "path": "/forward_netbox_validation/forward_prefixes_ipv6",
                            },
                        },
                        "by_query_id": {
                            "Q_ipv4": [
                                {
                                    "queryId": "Q_ipv4",
                                    "path": "/forward_netbox_validation/forward_prefixes_ipv4",
                                }
                            ],
                            "Q_ipv6": [
                                {
                                    "queryId": "Q_ipv6",
                                    "path": "/forward_netbox_validation/forward_prefixes_ipv6",
                                }
                            ],
                        },
                    }
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
        self.assertEqual(query_map.query_id, "")
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
                get_nqe_repository_query_index=Mock(
                    return_value={
                        "rows": [
                            {
                                "queryId": "Q_interfaces",
                                "path": "/forward_netbox_validation/forward_interfaces",
                            }
                        ],
                        "by_path": {
                            "/forward_netbox_validation/forward_interfaces": {
                                "queryId": "Q_interfaces",
                                "path": "/forward_netbox_validation/forward_interfaces",
                            }
                        },
                        "by_query_id": {
                            "Q_interfaces": [
                                {
                                    "queryId": "Q_interfaces",
                                    "path": "/forward_netbox_validation/forward_interfaces",
                                }
                            ]
                        },
                    }
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
