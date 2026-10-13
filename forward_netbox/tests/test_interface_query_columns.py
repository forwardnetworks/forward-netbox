import re

from django.test import SimpleTestCase

from forward_netbox.utilities.query_registry import read_builtin_query_source

# The columns the interface apply path reads from a row (apply_engine_bulk
# `bulk_orm_apply_interface`, sync_interface.apply_dcim_interface).
INTERFACE_APPLY_COLUMNS = {
    "device",
    "name",
    "type",
    "enabled",
    "lag",
    "mode",
    "untagged_vlan",
    "mtu",
    "speed",
    "description",
    "parent",
}


def _record_fields(block):
    """Top-level field names of one `select { ... }` record."""
    depth = 0
    fields = []
    for line in block.splitlines():
        stripped = line.strip()
        if depth == 0:
            match = re.match(r"^(\w+):", stripped)
            if match:
                fields.append(match.group(1))
        depth += sum(line.count(c) for c in "([{") - sum(line.count(c) for c in ")]}")
    return set(fields)


def _select_records(source):
    """Map each named definition to the field set of its `select { ... }` record."""
    records = {}
    for chunk in re.split(r"\n(?=\w+\(forward_netbox_shard_keys)", source):
        name = chunk.split("(", 1)[0].strip()
        match = re.search(r"select \{\n(.*?)\n\s*\};", chunk, re.S)
        if match:
            records[name] = _record_fields(match.group(1))
    return records


class InterfaceQueryColumnsTest(SimpleTestCase):
    # NQE keeps only the columns every member of a `+` union shares, so one
    # member selecting fewer fields silently strips the rest from every row:
    # `lag`, `mode`, `untagged_vlan`, `mtu`, `speed` and `description` never
    # reached NetBox once the endpoint branch (device, name, type, enabled) was
    # unioned in. Every branch that feeds the final union must therefore select
    # the same columns, null-padded where it has no value.
    def test_every_union_branch_selects_every_column_the_apply_reads(self):
        records = _select_records(read_builtin_query_source("forward_interfaces.nqe"))

        for branch in (
            "device_interfaces",
            "subinterface_interfaces",
            "endpoint_interfaces",
        ):
            self.assertIn(branch, records, f"{branch} select record not found")
            self.assertEqual(
                records[branch],
                INTERFACE_APPLY_COLUMNS,
                f"{branch} must select exactly the columns the apply reads, or "
                "the union drops the others from every row.",
            )
