from io import StringIO
from unittest.mock import Mock
from unittest.mock import patch

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase

from forward_netbox.utilities.query_binding_resolution import (
    builtin_query_repository_sync_summary,
)
from forward_netbox.utilities.query_registry import read_compiled_builtin_query_source


class ValidationOrgQueryAuditTest(TestCase):
    def _query_defaults(self):
        return [
            {
                "model_string": "dcim.device",
                "name": "Forward Devices",
                "filename": "forward_devices.nqe",
            },
            {
                "model_string": "dcim.interface",
                "name": "Forward Interfaces",
                "filename": "forward_interfaces.nqe",
            },
        ]

    def test_builtin_query_repository_sync_summary_reports_pass_for_matching_sources(
        self,
    ):
        query_defaults = self._query_defaults()
        client = Mock()
        client.get_nqe_repository_query_index.return_value = {
            "rows": [
                {
                    "queryId": "Q_devices",
                    "path": "/forward_netbox_validation/forward_devices",
                    "lastCommitId": "commit-1",
                },
                {
                    "queryId": "Q_interfaces",
                    "path": "/forward_netbox_validation/forward_interfaces",
                    "lastCommitId": "commit-2",
                },
            ],
            "by_path": {
                "/forward_netbox_validation/forward_devices": {
                    "queryId": "Q_devices",
                    "path": "/forward_netbox_validation/forward_devices",
                    "lastCommitId": "commit-1",
                },
                "/forward_netbox_validation/forward_interfaces": {
                    "queryId": "Q_interfaces",
                    "path": "/forward_netbox_validation/forward_interfaces",
                    "lastCommitId": "commit-2",
                },
            },
        }

        def committed_query(repository, query_path, commit_id, query_index=None):
            filename = query_path.rsplit("/", 1)[-1] + ".nqe"
            query_id = "Q_devices" if "devices" in query_path else "Q_interfaces"
            commit = "commit-1" if "devices" in query_path else "commit-2"
            return {
                "queryId": query_id,
                "query": read_compiled_builtin_query_source(filename),
                "lastCommitId": commit,
                "path": query_path,
            }

        client.get_committed_nqe_query.side_effect = committed_query

        with patch(
            "forward_netbox.utilities.query_binding_resolution.query_contract_summary_for_maps"
        ) as query_contract_summary_for_maps:
            query_contract_summary_for_maps.return_value = {
                "status": "pass",
                "model_count": len(query_defaults),
                "models": {},
                "gaps": [],
            }
            report = builtin_query_repository_sync_summary(
                client=client,
                repository="org",
                directory="/forward_netbox_validation/",
                query_defaults=query_defaults,
            )

        self.assertEqual(report["status"], "pass")
        self.assertEqual(report["gate_status"], "proved")
        self.assertEqual(
            report["gate_message"],
            "Validation org query folder matches bundled compiled NQE source.",
        )
        self.assertEqual(report["query_count"], 2)
        self.assertEqual(report["matched_count"], 2)
        self.assertEqual(report["missing_count"], 0)
        self.assertEqual(report["stale_count"], 0)
        self.assertEqual(report["lookup_error_count"], 0)
        self.assertEqual(report["remediation_action_counts"], {})
        self.assertEqual(report["query_contract_summary"]["status"], "pass")
        client.get_nqe_repository_query_index.assert_called_once_with(
            repository="org",
            directory="/forward_netbox_validation",
        )
        self.assertEqual(client.get_committed_nqe_query.call_count, 2)

    def test_builtin_query_repository_sync_summary_resolves_commit_from_history(self):
        query_defaults = self._query_defaults()
        client = Mock()
        client.get_nqe_repository_query_index.return_value = {
            "rows": [
                {
                    "queryId": "Q_devices",
                    "path": "/forward_netbox_validation/forward_devices",
                }
            ],
            "by_path": {
                "/forward_netbox_validation/forward_devices": {
                    "queryId": "Q_devices",
                    "path": "/forward_netbox_validation/forward_devices",
                }
            },
        }
        client.get_nqe_query_history.return_value = [{"id": "commit-history"}]
        client.get_committed_nqe_query.return_value = {
            "queryId": "Q_devices",
            "sourceCode": read_compiled_builtin_query_source("forward_devices.nqe"),
            "lastCommitId": "commit-history",
            "path": "/forward_netbox_validation/forward_devices",
        }

        with patch(
            "forward_netbox.utilities.query_binding_resolution.query_contract_summary_for_maps"
        ) as query_contract_summary_for_maps:
            query_contract_summary_for_maps.return_value = {
                "status": "pass",
                "model_count": 1,
                "models": {},
                "gaps": [],
            }
            report = builtin_query_repository_sync_summary(
                client=client,
                repository="org",
                directory="/forward_netbox_validation/",
                query_defaults=query_defaults[:1],
            )

        self.assertEqual(report["status"], "pass")
        self.assertEqual(report["matched_count"], 1)
        client.get_nqe_query_history.assert_called_once_with("Q_devices")
        client.get_committed_nqe_query.assert_called_once_with(
            repository="org",
            query_path="/forward_netbox_validation/forward_devices",
            commit_id="commit-history",
            query_index=client.get_nqe_repository_query_index.return_value,
        )

    def test_builtin_query_repository_sync_summary_reports_missing_and_stale(self):
        query_defaults = self._query_defaults()
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
        }
        client.get_committed_nqe_query.return_value = {
            "queryId": "Q_devices",
            "query": 'select {name: "changed"}',
            "lastCommitId": "commit-1",
            "path": "/forward_netbox_validation/forward_devices",
        }

        with patch(
            "forward_netbox.utilities.query_binding_resolution.query_contract_summary_for_maps"
        ) as query_contract_summary_for_maps:
            query_contract_summary_for_maps.return_value = {
                "status": "pass",
                "model_count": len(query_defaults),
                "models": {},
                "gaps": [],
            }
            report = builtin_query_repository_sync_summary(
                client=client,
                repository="org",
                directory="/forward_netbox_validation/",
                query_defaults=query_defaults,
            )

        self.assertEqual(report["status"], "fail")
        self.assertEqual(report["gate_status"], "unproved")
        self.assertEqual(report["missing_count"], 1)
        self.assertEqual(report["stale_count"], 1)
        self.assertEqual(report["matched_count"], 0)
        self.assertEqual(len(report["gaps"]), 2)
        self.assertEqual(
            report["remediation_action_counts"],
            {"publish_bundled_queries": 2},
        )
        self.assertEqual(
            report["missing"][0]["expected_path"],
            "/forward_netbox_validation/forward_interfaces",
        )
        self.assertEqual(
            report["stale"][0]["expected_path"],
            "/forward_netbox_validation/forward_devices",
        )

    def test_builtin_query_repository_sync_summary_treats_missing_source_as_gap(
        self,
    ):
        query_defaults = self._query_defaults()
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
        }
        client.get_committed_nqe_query.return_value = {
            "queryId": "Q_devices",
            "path": "/forward_netbox_validation/forward_devices",
            "lastCommitId": "commit-1",
        }

        with patch(
            "forward_netbox.utilities.query_binding_resolution.query_contract_summary_for_maps"
        ) as query_contract_summary_for_maps:
            query_contract_summary_for_maps.return_value = {
                "status": "pass",
                "model_count": len(query_defaults),
                "models": {},
                "gaps": [],
            }
            report = builtin_query_repository_sync_summary(
                client=client,
                repository="org",
                directory="/forward_netbox_validation/",
                query_defaults=query_defaults[:1],
            )

        self.assertEqual(report["status"], "fail")
        self.assertEqual(report["gate_status"], "unproved")
        self.assertEqual(report["missing_count"], 0)
        self.assertEqual(report["stale_count"], 0)
        self.assertEqual(report["source_unavailable_count"], 1)
        self.assertEqual(report["matched_count"], 0)
        self.assertEqual(len(report["gaps"]), 1)
        self.assertEqual(
            report["gaps"][0]["code"],
            "published_query_source_unavailable",
        )
        self.assertEqual(
            report["remediation_action_counts"],
            {"publish_bundled_queries": 1},
        )
        self.assertEqual(
            report["source_unavailable"][0]["expected_path"],
            "/forward_netbox_validation/forward_devices",
        )

    def test_command_repairs_then_reports_and_fails_on_gap(self):
        source_client = Mock()
        source_client.get_nqe_repository_query_index.return_value = {
            "rows": [],
            "by_path": {},
        }
        source_client.get_committed_nqe_query.return_value = {
            "queryId": "Q_devices",
            "query": read_compiled_builtin_query_source("forward_devices.nqe"),
            "lastCommitId": "commit-1",
            "path": "/forward_netbox_validation/forward_devices",
        }

        with patch(
            "forward_netbox.management.commands.forward_validation_org_query_audit.ForwardSource.validate_connection"
        ) as validate_connection, patch(
            "forward_netbox.management.commands.forward_validation_org_query_audit.ForwardSource.get_client",
            return_value=source_client,
        ), patch(
            "forward_netbox.management.commands.forward_validation_org_query_audit.publish_builtin_nqe_map_queries"
        ) as publish_builtin_nqe_map_queries, patch(
            "forward_netbox.management.commands.forward_validation_org_query_audit.builtin_query_repository_sync_summary"
        ) as builtin_query_repository_sync_summary:
            validate_connection.return_value = None
            publish_builtin_nqe_map_queries.return_value = []
            builtin_query_repository_sync_summary.return_value = {
                "status": "pass",
                "repository": "org",
                "directory": "/forward_netbox_validation",
                "query_count": 1,
                "published_count": 1,
                "matched_count": 1,
                "missing_count": 0,
                "stale_count": 0,
                "lookup_error_count": 0,
                "query_contract_summary": {"status": "pass", "gaps": []},
                "matched": [],
                "missing": [],
                "stale": [],
                "lookup_errors": [],
                "gaps": [],
            }

            stream = StringIO()
            call_command(
                "forward_validation_org_query_audit",
                "--source-name",
                "validation-source",
                "--url",
                "https://fwd.app",
                "--username",
                "user@example.com",
                "--password",
                "secret",
                "--network-id",
                "network-1",
                "--repair",
                stdout=stream,
            )

        publish_builtin_nqe_map_queries.assert_called_once()
        self.assertFalse(publish_builtin_nqe_map_queries.call_args.kwargs["overwrite"])
        builtin_query_repository_sync_summary.assert_called_once()
        self.assertIn('"status": "pass"', stream.getvalue())

    def test_command_fail_on_gap_raises(self):
        source_client = Mock()
        source_client.get_nqe_repository_query_index.return_value = {
            "rows": [],
            "by_path": {},
        }

        with patch(
            "forward_netbox.management.commands.forward_validation_org_query_audit.ForwardSource.validate_connection"
        ) as validate_connection, patch(
            "forward_netbox.management.commands.forward_validation_org_query_audit.ForwardSource.get_client",
            return_value=source_client,
        ), patch(
            "forward_netbox.management.commands.forward_validation_org_query_audit.builtin_query_repository_sync_summary"
        ) as builtin_query_repository_sync_summary:
            validate_connection.return_value = None
            builtin_query_repository_sync_summary.return_value = {
                "status": "fail",
                "repository": "org",
                "directory": "/forward_netbox_validation",
                "query_count": 1,
                "published_count": 0,
                "matched_count": 0,
                "missing_count": 1,
                "stale_count": 0,
                "lookup_error_count": 0,
                "query_contract_summary": {"status": "pass", "gaps": []},
                "matched": [],
                "missing": [
                    {"expected_path": "/forward_netbox_validation/forward_devices"}
                ],
                "stale": [],
                "lookup_errors": [],
                "gaps": [{"code": "missing_published_query_path"}],
            }

            with self.assertRaises(CommandError):
                call_command(
                    "forward_validation_org_query_audit",
                    "--source-name",
                    "validation-source",
                    "--url",
                    "https://fwd.app",
                    "--username",
                    "user@example.com",
                    "--password",
                    "secret",
                    "--network-id",
                    "network-1",
                    "--fail-on-gap",
                )
