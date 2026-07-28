"""A map must resolve a head commit whether it is bound by path or by query ID.

Reproduces the customer-reported 2.6.3 failure. `Resolve to Query ID` moves a
map's path into `resolved_query_path` and clears `query_path`. `QuerySpec.resolve`
only looked at `query_path`, so an ID-bound map never received a commit, the
execution contract rejected it as `unresolved_full_commit`, and every model was
skipped - producing an empty workload that was reported as a converged sync.
"""

from django.test import SimpleTestCase

from forward_netbox.utilities.query_registry import QuerySpec

HEAD = "c0ffee1234"
OTHER_HEAD = "deadbeef99"
QUERY_ID = "Q_bound"


class _Client:
    """Minimal stand-in for the Forward client's two resolution paths."""

    def __init__(self, *, by_path=None, head_for_id=None):
        self._by_path = by_path or {}
        self._head_for_id = head_for_id or {}
        self.path_lookups = []
        self.id_lookups = []

    def get_committed_nqe_query(
        self, *, repository="org", query_path="", commit_id="head", query_index=None
    ):
        self.path_lookups.append(query_path)
        return dict(self._by_path.get(query_path) or {})

    def resolve_nqe_query_head_commit(
        self, *, query_id, repository="org", query_index=None
    ):
        self.id_lookups.append(query_id)
        return self._head_for_id.get(query_id, "")


class PathBoundResolutionTest(SimpleTestCase):
    def test_path_bound_map_still_resolves_head(self):
        client = _Client(
            by_path={"/f/devices": {"queryId": "Q_path", "lastCommitId": HEAD}}
        )
        spec = QuerySpec(
            model_string="dcim.device",
            query_name="Forward Devices",
            query_repository="org",
            query_path="/f/devices",
        )
        resolved = spec.resolve(client)
        self.assertEqual(resolved.commit_id, HEAD)
        self.assertEqual(client.path_lookups, ["/f/devices"])


class IdBoundResolutionTest(SimpleTestCase):
    def _spec(self, **overrides):
        fields = {
            "model_string": "dcim.device",
            "query_name": "Forward Devices",
            "query_id": QUERY_ID,
            "query_repository": "org",
            "query_path": None,
            "resolved_query_path": "/f/devices",
        }
        fields.update(overrides)
        return QuerySpec(**fields)

    def test_id_bound_map_resolves_head_through_its_retained_path(self):
        # The customer case: query_path cleared, path kept in resolved_query_path.
        client = _Client(
            by_path={"/f/devices": {"queryId": QUERY_ID, "lastCommitId": HEAD}}
        )
        self.assertEqual(self._spec().resolve(client).commit_id, HEAD)

    def test_commit_from_a_different_query_is_not_adopted(self):
        # The path now resolves to another query; its head is not ours.
        client = _Client(
            by_path={"/f/devices": {"queryId": "Q_other", "lastCommitId": OTHER_HEAD}}
        )
        self.assertFalse(self._spec().resolve(client).commit_id)

    def test_id_only_binding_resolves_head_from_the_repository_index(self):
        client = _Client(head_for_id={QUERY_ID: HEAD})
        resolved = self._spec(resolved_query_path=None).resolve(client)
        self.assertEqual(resolved.commit_id, HEAD)
        self.assertEqual(client.id_lookups, [QUERY_ID])

    def test_unresolvable_id_leaves_the_spec_unchanged(self):
        client = _Client(head_for_id={})
        self.assertFalse(self._spec(resolved_query_path=None).resolve(client).commit_id)

    def test_a_pinned_commit_is_never_overwritten(self):
        client = _Client(head_for_id={QUERY_ID: HEAD})
        resolved = self._spec(resolved_query_path=None, commit_id="pinned").resolve(
            client
        )
        self.assertEqual(resolved.commit_id, "pinned")
        self.assertEqual(client.id_lookups, [])

    def test_a_failing_repository_lookup_does_not_raise(self):
        class _Failing(_Client):
            def resolve_nqe_query_head_commit(self, **kwargs):
                raise RuntimeError("repository unavailable")

        resolved = self._spec(resolved_query_path=None).resolve(_Failing())
        self.assertFalse(resolved.commit_id)

    def test_client_without_the_resolver_is_tolerated(self):
        class _Old:
            def get_committed_nqe_query(self, **kwargs):
                return {}

        self.assertFalse(self._spec(resolved_query_path=None).resolve(_Old()).commit_id)
