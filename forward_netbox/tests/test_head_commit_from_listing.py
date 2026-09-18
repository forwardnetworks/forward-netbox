"""An org query's head commit must come from the endpoint that reports one.

The repository directory listing returns only intent/path/queryId/repository -
no commit field at all. Returning one of those rows left the caller with an
empty commit, the execution contract rejected the map as
`unresolved_full_commit`, and a customer's whole sync fetched nothing while
reporting success. The commits endpoint does report `lastCommitId`, so a
commit-less index row must fall through to it rather than short-circuit.
"""

from types import SimpleNamespace
from unittest.mock import Mock

from django.test import TestCase

from forward_netbox.utilities import forward_api_impl
from forward_netbox.utilities.forward_api import ForwardClient
from forward_netbox.utilities.crypto import encrypt_secret

PATH = "/Nested_Folder/forward_netbox_validation/forward_locations"
QUERY_ID = "Q_bound"
HEAD = "commit_head_value"

# Exactly the shape the live directory listing returns for an org query.
LISTING_ROW = {
    "intent": "",
    "path": PATH,
    "queryId": QUERY_ID,
    "repository": "org",
}


class HeadCommitResolutionTest(TestCase):
    def setUp(self):
        shared_cache = forward_api_impl._shared_read_cache()
        if hasattr(shared_cache, "clear"):
            shared_cache.clear()
        self.client = ForwardClient(
            SimpleNamespace(
                url="https://fwd.app",
                parameters={
                    "username": "user@example.com",
                    "password": encrypt_secret("secret"),
                    "verify": True,
                    "timeout": 1200,
                },
            )
        )

    def _index(self, row):
        return {
            "rows": [dict(row)],
            "by_path": {str(row["path"]): dict(row)},
            "by_query_id": {str(row.get("queryId")): [dict(row)]},
        }

    def _committed_query(self):
        return SimpleNamespace(
            query_id=QUERY_ID,
            path=PATH,
            intent="",
            last_commit_id=HEAD,
            last_commit=None,
            source_code=None,
        )

    def test_commitless_listing_row_falls_through_and_resolves_head(self):
        # The customer case: the row exists but carries no commit.
        self.client._sdk_client.nqe.repo.queries = Mock(
            return_value=[self._committed_query()]
        )
        resolved = forward_api_impl.get_committed_nqe_query(
            self.client,
            repository="org",
            query_path=PATH,
            commit_id="head",
            query_index=self._index(LISTING_ROW),
        )
        self.assertEqual(resolved.get("lastCommitId"), HEAD)
        self.assertEqual(resolved.get("queryId"), QUERY_ID)
        self.client._sdk_client.nqe.repo.queries.assert_called_once()

    def test_a_listing_row_carrying_a_commit_needs_no_request(self):
        row = dict(LISTING_ROW, lastCommitId=HEAD)
        self.client._sdk_client.nqe.repo.queries = Mock()
        resolved = forward_api_impl.get_committed_nqe_query(
            self.client,
            repository="org",
            query_path=PATH,
            commit_id="head",
            query_index=self._index(row),
        )
        self.assertEqual(resolved.get("lastCommitId"), HEAD)
        self.client._sdk_client.nqe.repo.queries.assert_not_called()

    def test_the_fallthrough_requests_the_head_commit(self):
        self.client._sdk_client.nqe.repo.queries = Mock(
            return_value=[self._committed_query()]
        )
        forward_api_impl.get_committed_nqe_query(
            self.client,
            repository="org",
            query_path=PATH,
            commit_id="head",
            query_index=self._index(LISTING_ROW),
        )
        self.client._sdk_client.nqe.repo.queries.assert_called_once_with(
            repository="org", commit_id="head", path=PATH, with_source=True
        )

    def test_an_explicitly_pinned_commit_is_still_honoured(self):
        self.client._sdk_client.nqe.repo.queries = Mock(
            return_value=[self._committed_query()]
        )
        forward_api_impl.get_committed_nqe_query(
            self.client,
            repository="org",
            query_path=PATH,
            commit_id="pinned_commit",
            query_index=self._index(LISTING_ROW),
        )
        self.client._sdk_client.nqe.repo.queries.assert_called_once_with(
            repository="org", commit_id="pinned_commit", path=PATH, with_source=True
        )
