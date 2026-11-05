# NetBox queues one search-index job per savepoint scope, and every row of a sync
# or merge runs in its own savepoint, so a large run queued thousands. The plugin's
# jobs merge NetBox's committed index work and hand it over in a few bounded
# batches. Only committed work is ever collected: a rolled-back savepoint's never
# reaches NetBox's flush.
from unittest.mock import patch

from dcim.models import Site
from django.db import transaction
from django.test import SimpleTestCase
from django.test import TransactionTestCase
from netbox.search import deferred

from forward_netbox.utilities import search_batching
from forward_netbox.utilities.search_batching import batched_search_indexing
from forward_netbox.utilities.search_batching import FLUSH_CHUNK_SIZE


class BufferTest(SimpleTestCase):
    def _run(self, batches):
        handed = []

        def real(batch, alias):
            handed.append((dict(batch), alias))

        with patch.object(deferred, "_flush", real):
            with batched_search_indexing():
                for batch, alias in batches:
                    deferred._flush(batch, alias)
        return handed

    def test_many_committed_batches_go_out_as_one(self):
        handed = self._run(
            [({(5, pk): deferred.OP_CACHE}, "default") for pk in range(1, 201)]
        )

        self.assertEqual(len(handed), 1)
        self.assertEqual(len(handed[0][0]), 200)

    def test_a_later_commit_wins(self):
        handed = self._run(
            [
                ({(5, 1): deferred.OP_REMOVE}, "default"),
                ({(5, 1): deferred.OP_CACHE}, "default"),
            ]
        )

        self.assertEqual(handed[0][0], {(5, 1): deferred.OP_CACHE})

    def test_a_full_buffer_is_handed_over_during_the_job(self):
        handed = self._run(
            [
                ({(5, pk): deferred.OP_CACHE}, "default")
                for pk in range(FLUSH_CHUNK_SIZE + 3)
            ]
        )

        self.assertEqual([len(batch) for batch, _ in handed], [FLUSH_CHUNK_SIZE, 3])

    def test_the_real_flush_is_restored_even_when_the_job_raises(self):
        real = deferred._flush
        with self.assertRaises(RuntimeError):
            with batched_search_indexing():
                raise RuntimeError("job failed")
        self.assertIs(deferred._flush, real)
        self.assertFalse(search_batching._active)

    def test_a_failed_handover_does_not_fail_the_job(self):
        with patch.object(deferred, "_flush", side_effect=RuntimeError("redis down")):
            with batched_search_indexing():
                deferred._flush({(5, 1): deferred.OP_CACHE}, "default")

    def test_a_gone_branch_schema_is_skipped_not_enqueued(self):
        with patch.object(search_batching, "_alias_usable", return_value=False):
            handed = self._run([({(5, 1): deferred.OP_CACHE}, "branch_gone")])

        self.assertEqual(handed, [])

    def test_nested_use_hands_over_once_at_the_outer_exit(self):
        handed = []
        with patch.object(deferred, "_flush", lambda b, a: handed.append(b)):
            with batched_search_indexing():
                with batched_search_indexing():
                    deferred._flush({(5, 1): deferred.OP_CACHE}, "default")
                self.assertEqual(handed, [])
        self.assertEqual(len(handed), 1)

    def test_an_unexpected_call_shape_is_passed_straight_through(self):
        calls = []
        with patch.object(deferred, "_flush", lambda *a, **k: calls.append((a, k))):
            with batched_search_indexing():
                deferred._flush({(5, 1): deferred.OP_CACHE}, "default", extra=1)
        self.assertEqual(len(calls), 1)


class CommittedWorkOnlyTest(TransactionTestCase):
    """The reported shape, against NetBox's real deferral: many saves, each in
    its own savepoint, inside one job; and a rolled-back delete."""

    def _capture(self):
        handed = []
        return handed, patch.object(
            deferred, "_flush", lambda batch, alias: handed.append(dict(batch))
        )

    def test_many_savepoint_saves_hand_over_one_batch(self):
        handed, patched = self._capture()
        with patched, batched_search_indexing():
            with transaction.atomic():
                for index in range(30):
                    with transaction.atomic():
                        Site.objects.create(name=f"bs-{index}", slug=f"bs-{index}")

        self.assertEqual(len(handed), 1)
        self.assertGreaterEqual(len(handed[0]), 30)

    def test_a_rolled_back_delete_never_reaches_the_index(self):
        site = Site.objects.create(name="kept", slug="kept")
        handed, patched = self._capture()
        with patched, batched_search_indexing():
            with transaction.atomic():
                try:
                    with transaction.atomic():
                        Site.objects.get(pk=site.pk).delete()
                        raise RuntimeError("refused")
                except RuntimeError:
                    pass

        self.assertTrue(Site.objects.filter(pk=site.pk).exists())
        removed = [
            key
            for batch in handed
            for key, op in batch.items()
            if op == deferred.OP_REMOVE and key[1] == site.pk
        ]
        self.assertEqual(removed, [])


class JobsAreWrappedTest(SimpleTestCase):
    def test_the_sync_and_merge_jobs_are_wrapped_and_keep_their_names(self):
        from forward_netbox import jobs

        for job in (jobs.sync_forwardsync, jobs.merge_forwardingestion):
            self.assertTrue(hasattr(job, "__wrapped__"))
            self.assertEqual(job.__module__, "forward_netbox.jobs")
        self.assertEqual(jobs.sync_forwardsync.__name__, "sync_forwardsync")

    def test_the_decorator_runs_the_body_inside_the_batching(self):
        from forward_netbox import jobs

        seen = {}

        @jobs._batch_search_indexing
        def body():
            seen["active"] = search_batching._active
            return "done"

        self.assertEqual(body(), "done")
        self.assertTrue(seen["active"])
        self.assertFalse(search_batching._active)


class JobQueryCacheTest(TransactionTestCase):
    """NetBox caches object-type and custom-field lookups only when a query
    cache is set; plugin jobs now set one for the run."""

    def test_object_type_lookups_hit_the_database_once_per_model(self):
        from django.db import connection
        from django.test.utils import CaptureQueriesContext

        from core.models import ObjectType
        from forward_netbox.utilities.search_batching import job_query_cache

        with job_query_cache():
            ObjectType.objects.get_for_model(Site)
            with CaptureQueriesContext(connection) as queries:
                for _ in range(20):
                    ObjectType.objects.get_for_model(Site)
        self.assertEqual(len(queries), 0)

    def test_without_it_every_lookup_queries(self):
        from django.db import connection
        from django.test.utils import CaptureQueriesContext

        from core.models import ObjectType

        with CaptureQueriesContext(connection) as queries:
            for _ in range(5):
                ObjectType.objects.get_for_model(Site)
        self.assertGreaterEqual(len(queries), 5)

    def test_an_existing_cache_is_left_alone_and_restored(self):
        from netbox.context import query_cache

        from forward_netbox.utilities.search_batching import job_query_cache

        outer = {"marker": {}}
        token = query_cache.set(outer)
        try:
            with job_query_cache():
                self.assertIs(query_cache.get(), outer)
        finally:
            query_cache.reset(token)
        with job_query_cache():
            self.assertIsNotNone(query_cache.get())
        self.assertIsNone(query_cache.get())


class MergeRowKeepsTheJobCacheTest(TransactionTestCase):
    def test_event_tracking_inside_a_job_reuses_the_job_cache(self):
        from django.db import connection
        from django.test import RequestFactory
        from django.test.utils import CaptureQueriesContext
        from netbox.context_managers import event_tracking

        from core.models import ObjectType
        from forward_netbox.utilities.search_batching import job_query_cache
        from forward_netbox.utilities.search_batching import keep_job_query_cache

        request = RequestFactory().get("/")
        request.id = None
        with job_query_cache():
            ObjectType.objects.get_for_model(Site)
            with CaptureQueriesContext(connection) as queries:
                for _ in range(10):
                    with event_tracking(request), keep_job_query_cache():
                        ObjectType.objects.get_for_model(Site)
        self.assertEqual(len(queries), 0)

    def test_without_a_job_cache_it_changes_nothing(self):
        from netbox.context import query_cache

        from forward_netbox.utilities.search_batching import keep_job_query_cache

        before = query_cache.get()
        with keep_job_query_cache():
            self.assertIs(query_cache.get(), before)


class AliasUsableTest(TransactionTestCase):
    def test_a_dropped_branch_schema_is_not_usable(self):
        # Its search_path falls back to main, so only the catalogue can tell.
        self.assertFalse(search_batching._alias_usable("schema_branch_gone0000"))

    def test_an_existing_schema_and_main_are_usable(self):
        self.assertTrue(search_batching._alias_usable("default"))
        self.assertTrue(search_batching._alias_usable("schema_public"))
