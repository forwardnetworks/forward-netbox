from unittest.mock import Mock

from django.test import TestCase

from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.query_fetch import ForwardQueryContext
from forward_netbox.utilities.query_fetch_execution import ForwardQueryFetcher


class ScopeMatchedTagsResolveTest(TestCase):
    """The inline scope resolver returns, per in-scope device, exactly the
    include tags that device carries (the apply_device_scope_tags per-device map),
    intersected at resolve time so the persisted payload stays small."""

    def _fetcher(self, client):
        source = ForwardSource.objects.create(
            name=f"smt-src-{ForwardSource.objects.count()}",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={
                "username": "u",
                "password": "p",
                "verify": True,
                "network_id": "net-1",
            },
        )
        sync = ForwardSync.objects.create(
            name=f"smt-sync-{source.pk}",
            source=source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        return ForwardQueryFetcher(sync=sync, client=client, logger_=Mock())

    def test_resolve_returns_per_device_intersection(self):
        client = Mock()
        client.run_nqe_query.return_value = [
            {"name": "d1", "site": "s", "tagNames": ["TagA", "TagX"]},
            {"name": "d2", "site": "s", "tagNames": ["TagB"]},
            {"name": "d3", "site": "s", "tagNames": ["TagA", "TagB"]},
            {"name": "d4", "site": "s", "tagNames": ["TagX"]},
        ]
        names, sites, matched, _failed = self._fetcher(client)._resolve_scoped_tag_scope(
            network_id="net-1",
            snapshot_id="snap",
            include_tags=["TagA", "TagB"],
            exclude_tags=[],
            include_match="any",
        )
        self.assertEqual(
            matched, {"d1": ["TagA"], "d2": ["TagB"], "d3": ["TagA", "TagB"]}
        )
        # d4 carries none of the include tags -> not in the map (no tagging).
        self.assertNotIn("d4", matched)
        # The query must request tagNames.
        query = client.run_nqe_query.call_args.kwargs["query"]
        self.assertIn("tagNames: device.tagNames", query)

    def test_resolve_no_tags_returns_empty_map(self):
        result = self._fetcher(Mock())._resolve_scoped_tag_scope(
            network_id="net-1",
            snapshot_id="snap",
            include_tags=[],
            exclude_tags=[],
            include_match="any",
        )
        self.assertEqual(result, (set(), set(), {}, False))

    def test_context_as_dict_carries_full_matched_map(self):
        ctx = ForwardQueryContext(
            network_id="net-1",
            snapshot_selector="latestProcessed",
            snapshot_id="snap",
            scoped_matched_tags={"d1": ["TagA"], "d2": ["TagB"]},
        )
        # The branch apply path reads this back — it must be the full map, not a count.
        self.assertEqual(
            ctx.as_dict()["scoped_matched_tags"], {"d1": ["TagA"], "d2": ["TagB"]}
        )
