"""A moved query must still resolve to a verified revision.

Moving a query to a new folder does not rewrite its history: every commit made
before the move still records the *old* path. Head resolution asked for the
query's *current* path at each historical commit, which the Forward API answers
with HTTP 404, so no revision produced verifiable source. The contract then
refused the map as `unresolved_full_commit` and the model was skipped.

Observed live: a customer who had reorganised their repository under a new root
folder had all 32 enabled maps report `unresolved_full_commit` and synced
nothing at all. Fetching an historical commit at the bound path returned 404;
the same commit at the path recorded in its own history row returned the source.

The fixture below mirrors that API behaviour exactly rather than approximating
it, because a mock more permissive than the live API is what let this ship.
"""

from unittest.mock import Mock

from django.contrib.contenttypes.models import ContentType
from django.test import TestCase

from forward_netbox.models import ForwardNQEMap
from forward_netbox.utilities.query_registry import _build_query_spec_from_map
from forward_netbox.utilities.query_registry import (
    _resolve_unpinned_builtin_full_revision,
)
from forward_netbox.utilities.query_registry import BUILTIN_QUERY_DEFAULTS
from forward_netbox.utilities.query_registry import read_compiled_builtin_query_source

QUERY_ID = "test-query-identifier"
OLD_PATH = "/forward_netbox_validation/forward_locations"
NEW_PATH = "/CustomerRoot/forward_netbox_validation/forward_locations"
COMMIT = "1111111111111111111111111111111111111111"


def _builtin_identity():
    for model_string, name in BUILTIN_QUERY_DEFAULTS:
        if model_string == "dcim.site":
            return model_string, name
    raise AssertionError("no built-in default for dcim.site")


class MovedQueryHeadResolutionTest(TestCase):
    def setUp(self):
        model_string, name = _builtin_identity()
        app_label, model = model_string.split(".", 1)
        self.filename = BUILTIN_QUERY_DEFAULTS[(model_string, name)]["filename"]
        self.shipped_source = read_compiled_builtin_query_source(self.filename)
        self.query_map = ForwardNQEMap(
            name=name,
            netbox_model=ContentType.objects.get(app_label=app_label, model=model),
            built_in=True,
            query_id=QUERY_ID,
            query_path=NEW_PATH,
            query_repository="org",
            commit_id="",
            enabled=True,
        )

    def _client(self, *, history_path, serve_paths):
        """A client that answers only for `serve_paths`, and 404s otherwise.

        This is the live behaviour: a commit predating a move does not contain
        the query at its current path.
        """
        client = Mock()
        client.get_nqe_query_history.return_value = [
            {"id": COMMIT, "path": history_path}
        ]

        def committed(*, repository, query_path, commit_id, **kwargs):
            if query_path not in serve_paths:
                raise RuntimeError(f"HTTP 404 for {query_path} at {commit_id}")
            return {
                "queryId": QUERY_ID,
                "commitId": commit_id,
                "sourceCode": self.shipped_source,
            }

        client.get_committed_nqe_query.side_effect = committed
        return client

    def test_a_moved_query_resolves_through_its_historical_path(self):
        # The exact reported failure: bound at the new path, history at the old.
        spec = _build_query_spec_from_map(self.query_map)
        client = self._client(history_path=OLD_PATH, serve_paths={OLD_PATH})

        resolved = _resolve_unpinned_builtin_full_revision(spec, client)

        self.assertEqual(
            resolved.commit_id,
            COMMIT,
            "a query whose history predates a folder move must still resolve; "
            "otherwise every model reports unresolved_full_commit",
        )

    def test_the_current_path_is_still_preferred_when_it_serves(self):
        spec = _build_query_spec_from_map(self.query_map)
        client = self._client(history_path=NEW_PATH, serve_paths={NEW_PATH})

        resolved = _resolve_unpinned_builtin_full_revision(spec, client)

        self.assertEqual(resolved.commit_id, COMMIT)

    def test_an_unmoved_query_is_unaffected(self):
        # Both paths identical — the ordinary case must not change behaviour.
        spec = _build_query_spec_from_map(self.query_map)
        client = self._client(history_path=NEW_PATH, serve_paths={NEW_PATH, OLD_PATH})

        resolved = _resolve_unpinned_builtin_full_revision(spec, client)

        self.assertEqual(resolved.commit_id, COMMIT)

    def test_a_genuinely_unresolvable_query_still_fails_closed(self):
        # No path serves: the contract must stay refused, not adopt a commit.
        spec = _build_query_spec_from_map(self.query_map)
        client = self._client(history_path=OLD_PATH, serve_paths=set())

        resolved = _resolve_unpinned_builtin_full_revision(spec, client)

        self.assertIsNone(resolved.commit_id)

    def test_a_revision_whose_source_differs_is_refused(self):
        # Source verification must remain strict; a moved path is not a licence
        # to accept the wrong source.
        spec = _build_query_spec_from_map(self.query_map)
        client = self._client(history_path=OLD_PATH, serve_paths={OLD_PATH})
        client.get_committed_nqe_query.side_effect = lambda **kw: {
            "queryId": QUERY_ID,
            "commitId": kw["commit_id"],
            "sourceCode": "select { wrong: 1 }",
        }

        resolved = _resolve_unpinned_builtin_full_revision(spec, client)

        self.assertIsNone(resolved.commit_id)

    def test_a_revision_owned_by_another_query_is_refused(self):
        spec = _build_query_spec_from_map(self.query_map)
        client = self._client(history_path=OLD_PATH, serve_paths={OLD_PATH})
        client.get_committed_nqe_query.side_effect = lambda **kw: {
            "queryId": "Q_someone_else",
            "commitId": kw["commit_id"],
            "sourceCode": self.shipped_source,
        }

        resolved = _resolve_unpinned_builtin_full_revision(spec, client)

        self.assertIsNone(resolved.commit_id)

    def test_a_builtin_map_bound_by_query_id_keeps_it_on_the_spec(self):
        spec = _build_query_spec_from_map(self.query_map)
        self.assertTrue(spec.built_in)
        self.assertEqual(spec.run_query_id, QUERY_ID)
