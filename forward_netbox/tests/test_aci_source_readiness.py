# Every ACI map is a regex over raw command text, so zero rows has three
# causes with three remedies: the source command is not collected on any
# device, it is collected but its output is empty, or it is collected with
# output the regex no longer matches. Left alone, all three read as "no ACI
# hardware". This diagnostic tells them apart, and runs only when there is
# an empty ACI map to explain.
from types import SimpleNamespace
from unittest.mock import Mock

from django.test import SimpleTestCase

from forward_netbox.utilities.query_diagnostics import ACI_MAP_SOURCES
from forward_netbox.utilities.query_diagnostics import append_aci_source_diagnostics
from forward_netbox.utilities.query_diagnostics import (
    summarize_aci_source_readiness_rows,
)
from forward_netbox.utilities.query_fetch_execution import ForwardModelResult
from forward_netbox.utilities.query_registry import BUILTIN_OPTIONAL_QUERY_MAPS


def _result(query_name, model_string, row_count, failure_count=0):
    return ForwardModelResult(
        model_string=model_string,
        query_name=query_name,
        execution_mode="query",
        execution_value="",
        sync_mode="full",
        row_count=row_count,
        failure_count=failure_count,
    )


def _fetcher(results, readiness_rows):
    client = Mock()
    client.run_nqe_query.return_value = readiness_rows
    return SimpleNamespace(
        model_results=results, client=client, logger=Mock(), sync=object()
    )


_CONTEXT = SimpleNamespace(
    network_id="n", snapshot_id="s", query_parameters={"forward_netbox_shard_keys": []}
)


class AciSourceReadinessTest(SimpleTestCase):
    def test_every_bundled_aci_map_declares_its_source(self):
        aci_maps = {
            entry["name"]
            for entry in BUILTIN_OPTIONAL_QUERY_MAPS
            if entry["filename"].startswith("forward_aci_")
            and entry["name"] != "Forward ACI Fabrics"
            and entry["name"] != "Forward ACI Command Inventory"
        }
        self.assertEqual(aci_maps - set(ACI_MAP_SOURCES), set())

    def test_nothing_runs_when_every_aci_map_returned_rows(self):
        fetcher = _fetcher(
            [_result("Forward ACI Pods", "netbox_cisco_aci.acipod", 3)], []
        )
        append_aci_source_diagnostics(fetcher, _CONTEXT)
        fetcher.client.run_nqe_query.assert_not_called()

    def test_a_failed_map_is_not_explained_as_empty(self):
        fetcher = _fetcher(
            [
                _result(
                    "Forward ACI Pods", "netbox_cisco_aci.acipod", 0, failure_count=1
                )
            ],
            [],
        )
        append_aci_source_diagnostics(fetcher, _CONTEXT)
        fetcher.client.run_nqe_query.assert_not_called()

    def test_a_missing_source_is_named_with_its_sibling(self):
        rows = [
            {"source": "apic_detail", "device": "apic-1", "has_output": True},
            {"source": "apic_detail", "device": "apic-2", "has_output": True},
        ]
        fetcher = _fetcher(
            [_result("Forward ACI Pods", "netbox_cisco_aci.acipod", 0)], rows
        )
        append_aci_source_diagnostics(fetcher, _CONTEXT)

        fetcher.client.run_nqe_query.assert_called_once()
        message = fetcher.logger.log_warning.call_args.args[0]
        self.assertIn(
            "no completed device in this snapshot carries CISCO_ACI_FABRIC_NODES",
            message,
        )
        self.assertIn("`Forward ACI APIC Pods` reads", message)
        self.assertIn("present on 2 device(s)", message)
        diagnostic = fetcher.model_results[0].diagnostics[0]
        self.assertEqual(diagnostic["cause"], "source_not_collected")
        self.assertEqual(diagnostic["sibling"], "Forward ACI APIC Pods")

    def test_a_present_source_that_parsed_nothing_points_at_the_regex(self):
        rows = [{"source": "moquery_fvbd", "device": "apic-1", "has_output": True}]
        fetcher = _fetcher(
            [
                _result(
                    "Forward ACI Bridge Domains", "netbox_cisco_aci.acibridgedomain", 0
                )
            ],
            rows,
        )
        append_aci_source_diagnostics(fetcher, _CONTEXT)

        message = fetcher.logger.log_warning.call_args.args[0]
        self.assertIn("does not match what the map parses", message)
        self.assertEqual(
            fetcher.model_results[0].diagnostics[0]["cause"],
            "source_present_nothing_parsed",
        )

    def test_a_present_source_with_empty_output_is_its_own_cause(self):
        rows = [{"source": "moquery_eqptch", "device": "apic-1", "has_output": False}]
        fetcher = _fetcher(
            [_result("Forward ACI APIC CIMC Inventory", "dcim.inventoryitem", 0)], rows
        )
        append_aci_source_diagnostics(fetcher, _CONTEXT)

        self.assertEqual(
            fetcher.model_results[0].diagnostics[0]["cause"],
            "source_present_empty_output",
        )
        self.assertIn("output is empty", fetcher.logger.log_warning.call_args.args[0])

    def test_one_readiness_query_explains_every_empty_map(self):
        fetcher = _fetcher(
            [
                _result("Forward ACI Pods", "netbox_cisco_aci.acipod", 0),
                _result("Forward ACI Nodes", "netbox_cisco_aci.acinode", 0),
                _result("Forward ACI Tenants", "netbox_cisco_aci.acitenant", 4),
            ],
            [],
        )
        append_aci_source_diagnostics(fetcher, _CONTEXT)

        fetcher.client.run_nqe_query.assert_called_once()
        self.assertEqual(fetcher.logger.log_warning.call_count, 2)
        self.assertEqual(fetcher.model_results[2].diagnostics, [])

    def test_a_readiness_failure_is_a_warning_not_a_fetch_failure(self):
        fetcher = _fetcher(
            [_result("Forward ACI Pods", "netbox_cisco_aci.acipod", 0)], []
        )
        fetcher.client.run_nqe_query.side_effect = RuntimeError("boom")
        append_aci_source_diagnostics(fetcher, _CONTEXT)
        self.assertEqual(fetcher.model_results[0].diagnostics, [])
        self.assertIn("Unable to run", fetcher.logger.log_warning.call_args.args[0])

    def test_summary_counts_devices_not_rows(self):
        summary = summarize_aci_source_readiness_rows(
            [
                {"source": "fabric_nodes", "device": "leaf-1", "has_output": True},
                {"source": "fabric_nodes", "device": "leaf-1", "has_output": True},
                {"source": "fabric_nodes", "device": "leaf-2", "has_output": False},
            ]
        )
        self.assertEqual(summary, {"fabric_nodes": (2, 1)})
