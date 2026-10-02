import re
from pathlib import Path
from unittest import TestCase

from forward_netbox.utilities.query_execution_contract import (
    declared_query_parameters,
)
from forward_netbox.utilities.query_registry import BUILTIN_OPTIONAL_QUERY_MAPS
from forward_netbox.utilities.query_registry import QUERY_DIR


def _source():
    return Path(QUERY_DIR, "forward_static_routes.nqe").read_text(encoding="utf-8")


def _code():
    return re.sub(r"/\*.*?\*/", "", _source(), flags=re.S)


class StaticRoutesQueryContractTest(TestCase):
    def test_it_takes_only_the_shard_keys(self):
        declared = declared_query_parameters(_source())

        self.assertEqual([p.name for p in declared], ["forward_netbox_shard_keys"])

    def test_it_never_emits_an_empty_list_literal(self):
        self.assertNotIn("[]", _code())

    def test_it_reads_configured_lines_not_the_forwarding_table(self):
        code = _code()

        self.assertIn("device.files.config", code)
        self.assertNotIn("originProtocol", code)
        self.assertNotIn("ipv4Unicast", code)

    def test_it_covers_global_vrf_and_nxos_vrf_context_routes(self):
        code = _code()

        self.assertIn("(?<family>ip|ipv6) route", code)
        self.assertIn("vrf context", code)
        self.assertIn("(?:vrf (?<vrf>", code)

    def test_it_emits_the_arguments_verbatim_for_the_adapter_to_parse(self):
        code = _code()

        for column in ("device:", "os:", "vrf:", "family:", "args:"):
            self.assertIn(column, code)

    def test_it_is_limited_to_the_dialects_the_adapter_reads(self):
        self.assertIn("OS.IOS, OS.IOS_XE, OS.NXOS, OS.ARISTA_EOS", _code())


class StaticRoutesMapRegistrationTest(TestCase):
    def test_the_map_is_registered_and_opt_in(self):
        entries = [
            e
            for e in BUILTIN_OPTIONAL_QUERY_MAPS
            if e["model_string"] == "netbox_routing.staticroute"
        ]

        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["name"], "Forward Static Routes")
        self.assertEqual(entries[0]["filename"], "forward_static_routes.nqe")
        self.assertFalse(entries[0]["enabled"])
