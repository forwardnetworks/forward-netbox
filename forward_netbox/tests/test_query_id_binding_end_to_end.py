"""A customer map bound by query ID must resolve and become executable.

This is the primary binding, not an edge case: diff execution is query-ID-only,
and `Resolve to Query ID` deliberately produces exactly this shape. It had no
coverage against a realistic repository response, so two defects shipped:

* unpinned head resolution only ran for built-in specs, so a customer map kept
  an empty commit and the contract refused it as `unresolved_full_commit`
* the org directory listing carries no commit field at all, so any code path
  trusting it resolved nothing

Every fixture here mirrors the live API: the directory listing returns
intent/path/queryId/repository with no commit, and only the commits endpoint
reports `lastCommitId`.
"""

from django.test import SimpleTestCase

from forward_netbox.utilities.query_execution_contract import (
    resolve_execution_contract,
)
from forward_netbox.utilities.query_registry import QuerySpec
from forward_netbox.utilities.query_registry import resolve_query_specs_for_client

PATH = "/Customer_Folder/forward_netbox_validation/forward_locations"
QUERY_ID = "Q_customer"
HEAD = "9f28247c1b4e4d0a8f0b2c7d5e6a1b3c4d5e6f70"
SOURCE = "queries { devices { name } }"


class _Client:
    """Client mirroring the live split between listing and commits endpoints."""

    def __init__(self, *, query_id=QUERY_ID, commit=HEAD, source=SOURCE):
        self._query_id = query_id
        self._commit = commit
        self._source = source
        self.committed_lookups = []

    def get_committed_nqe_query(
        self, *, repository="org", query_path="", commit_id="head", **kwargs
    ):
        self.committed_lookups.append((query_path, commit_id))
        return {
            "path": query_path,
            "queryId": self._query_id,
            "lastCommitId": self._commit,
            "sourceCode": self._source,
            "repository": repository,
        }

    def get_nqe_repository_query_index(self, *, repository="org", directory="/"):
        # The org listing carries no commit field. Nothing may depend on one.
        row = {
            "intent": "",
            "path": PATH,
            "queryId": self._query_id,
            "repository": repository,
        }
        return {
            "rows": [row],
            "by_path": {PATH: dict(row)},
            "by_query_id": {self._query_id: [dict(row)]},
        }


def _spec(**overrides):
    fields = {
        "model_string": "dcim.site",
        "query_name": "Forward Locations",
        "query_id": QUERY_ID,
        "query_repository": "org",
        "query_path": None,
        "resolved_query_path": PATH,
        "built_in": False,
    }
    fields.update(overrides)
    return QuerySpec(**fields)


class QueryIdBindingResolutionTest(SimpleTestCase):
    def test_an_unpinned_customer_map_resolves_head(self):
        [resolved] = resolve_query_specs_for_client([_spec()], _Client())
        self.assertEqual(resolved.commit_id, HEAD)

    def test_the_resolved_contract_is_executable(self):
        # The whole point: a resolved map must not be refused as
        # unresolved_full_commit, which is what emptied a customer's sync.
        [resolved] = resolve_query_specs_for_client([_spec()], _Client())
        contract = resolve_execution_contract(resolved, effective_parameters={})
        self.assertNotEqual(contract.full_reason_code, "unresolved_full_commit")
        self.assertTrue(
            contract.full_eligible,
            f"contract refused the map: {contract.full_reason_code}",
        )

    def test_a_pinned_commit_is_preserved(self):
        [resolved] = resolve_query_specs_for_client(
            [_spec(commit_id="pinned_commit_value")], _Client()
        )
        self.assertEqual(resolved.commit_id, "pinned_commit_value")

    def test_a_path_resolving_to_another_query_is_refused(self):
        client = _Client(query_id="Q_someone_else")
        [resolved] = resolve_query_specs_for_client([_spec()], client)
        self.assertFalse(resolved.commit_id)

    def test_a_repository_failure_leaves_the_map_unresolved(self):
        class _Failing(_Client):
            def get_committed_nqe_query(self, **kwargs):
                raise RuntimeError("repository unavailable")

        [resolved] = resolve_query_specs_for_client([_spec()], _Failing())
        self.assertFalse(resolved.commit_id)

    def test_a_map_without_any_path_is_left_alone(self):
        [resolved] = resolve_query_specs_for_client(
            [_spec(resolved_query_path=None)], _Client()
        )
        self.assertFalse(resolved.commit_id)
