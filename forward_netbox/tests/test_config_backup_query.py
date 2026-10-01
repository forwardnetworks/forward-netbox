import re
from pathlib import Path
from unittest import TestCase

from forward_netbox.utilities.query_execution_contract import (
    declared_query_parameters,
)
from forward_netbox.utilities.query_registry import QUERY_DIR

# The config backup query is not a registered map and nothing runs it in a test:
# it only executes against Forward. These pin the properties that cannot be
# checked any other way, each one a way it has gone wrong or nearly did.


def _source():
    return Path(QUERY_DIR, "forward_config_backup.nqe").read_text(encoding="utf-8")


def _code():
    # The header comment explains the empty-list rule by naming it; strip it.
    return re.sub(r"/\*.*?\*/", "", _source(), flags=re.S)


class ConfigBackupQueryContractTest(TestCase):
    def test_the_job_passes_exactly_one_parameter(self):
        declared = declared_query_parameters(_source())

        self.assertEqual([p.name for p in declared], ["forward_netbox_shard_keys"])

    def test_every_branch_is_scoped_by_the_shard_keys(self):
        # An unscoped branch ships the whole collected estate and discards what
        # has nowhere to go - exactly what the scoping exists to prevent.
        code = _code()

        self.assertEqual(code.count("isEmpty(forward_netbox_shard_keys) ||"), 2)

    def test_it_never_emits_an_empty_list_literal(self):
        # Forward rejects `[]` at runtime ("Can't handle empty list literals
        # now!") while the query still lints clean.
        self.assertNotIn("[]", _code())


class ConfigBackupQuerySourcesTest(TestCase):
    def test_the_collected_command_comes_first_and_the_parsed_tree_is_the_fallback(
        self,
    ):
        code = _code()

        self.assertIn("command.commandType == CommandType.CONFIG", code)
        self.assertIn("device.files.config", code)
        self.assertIn("if isEmpty(collected) then [rendered] else collected", code)

    def test_empty_text_emits_no_row(self):
        self.assertIn('where text != ""', _code())

    def test_the_tree_is_rendered_to_the_depth_forward_produces(self):
        # Six levels is what every estate measured has. Anything deeper must be
        # visible in the file rather than silently dropped.
        code = _code()

        for variable in "abcdef":
            self.assertRegex(code, rf"foreach {variable} in ")
        self.assertIn("deeper configuration lines omitted", code)

    def test_the_render_needs_no_conditionals(self):
        # `join("", ...)` over no children is empty; an `if` per level is noise.
        self.assertNotRegex(_code(), r"if isEmpty\([a-f]\.children\)")

    def test_endpoints_are_included_only_through_a_configuration_style_command(self):
        code = _code()

        self.assertIn("network.endpoints", code)
        self.assertIn("endpoint.cliCommandResponses", code)
        for pattern in ("*running-config*", "show run*", "show config*"):
            self.assertIn(pattern, code)
        self.assertNotIn("snmpOutputs", code)

    def test_both_row_sources_produce_the_same_columns(self):
        code = _code()

        self.assertEqual(code.count("name: device.name"), 1)
        self.assertEqual(code.count("name: endpoint.name"), 1)
        self.assertEqual(code.count("config: text"), 2)
