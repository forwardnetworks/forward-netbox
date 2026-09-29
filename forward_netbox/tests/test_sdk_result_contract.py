"""The SDK result types we read, pinned to the attributes we actually use.

A `SimpleNamespace` stand-in accepts any attribute, so a test built on one
passes whatever the SDK really returns. `nqe.repo.queries()` returns a plain
dataclass carrying `commit_id` and `source`; the code once read the wire
model's `last_commit_id` and `source_code`, every test passed, and every
committed-query lookup raised `AttributeError` against a real Forward. These
read the real types, so a rename in an SDK bump fails here instead.
"""

import dataclasses
from unittest import TestCase

from forward_sdk.nqe.repository import RepositoryQuery


class SdkResultContractTest(TestCase):
    def test_repository_query_carries_the_fields_the_lookups_read(self):
        names = {field.name for field in dataclasses.fields(RepositoryQuery)}

        self.assertLessEqual(
            {"query_id", "path", "intent", "commit_id", "source", "repository"}, names
        )

    def test_repository_query_does_not_carry_the_wire_model_names(self):
        # The names the code used to read. Their absence is the reason a
        # stand-in that has them proves nothing.
        names = {field.name for field in dataclasses.fields(RepositoryQuery)}

        self.assertFalse({"last_commit_id", "last_commit", "source_code"} & names)

    def test_a_real_result_reads_without_error(self):
        query = RepositoryQuery(
            query_id="FQ_1",
            path="/netbox/forward_devices",
            commit_id="commit-1",
            intent="Forward Devices",
            source="select {}",
        )

        self.assertEqual(
            (query.query_id, query.commit_id, query.source),
            ("FQ_1", "commit-1", "select {}"),
        )
