# NetBox queues one search-index job per savepoint scope, and every row of a sync
# or merge runs in its own savepoint, so a large run queued thousands. The
# plugin's jobs collect that work and hand NetBox a few bounded batches instead.
from unittest.mock import patch

from django.db import transaction
from django.test import SimpleTestCase
from django.test import TestCase
from netbox.search import deferred

from forward_netbox.utilities import search_batching
from forward_netbox.utilities.search_batching import batched_search_indexing
from forward_netbox.utilities.search_batching import FLUSH_CHUNK_SIZE


class CollectionTest(SimpleTestCase):
    def _run(self, calls):
        flushed = []
        with patch.object(
            deferred,
            "_flush",
            side_effect=lambda batch, alias: flushed.append((batch, alias)),
        ):
            with batched_search_indexing():
                for args in calls:
                    deferred.mark_for_deferred_indexing(*args)
        return flushed

    def test_every_object_goes_out_in_one_batch_not_one_each(self):
        calls = [(5, pk, deferred.OP_CACHE, "default") for pk in range(1, 201)]

        flushed = self._run(calls)

        self.assertEqual(len(flushed), 1)
        self.assertEqual(len(flushed[0][0]), 200)

    def test_a_delete_wins_over_a_create_or_update(self):
        flushed = self._run(
            [
                (5, 1, deferred.OP_CACHE, "default"),
                (5, 1, deferred.OP_REMOVE, "default"),
                (5, 1, deferred.OP_CACHE, "default"),
            ]
        )

        self.assertEqual(flushed[0][0], {(5, 1): deferred.OP_REMOVE})

    def test_batches_are_bounded(self):
        calls = [
            (5, pk, deferred.OP_CACHE, "default")
            for pk in range(FLUSH_CHUNK_SIZE * 2 + 1)
        ]

        flushed = self._run(calls)

        self.assertEqual(
            [len(batch) for batch, _ in flushed],
            [FLUSH_CHUNK_SIZE, FLUSH_CHUNK_SIZE, 1],
        )

    def test_each_database_alias_is_flushed_to_itself(self):
        flushed = self._run(
            [
                (5, 1, deferred.OP_CACHE, "default"),
                (5, 2, deferred.OP_CACHE, "branch_x"),
            ]
        )

        self.assertEqual({alias for _, alias in flushed}, {"default", "branch_x"})

    def test_netboxs_own_function_is_restored_afterwards(self):
        original = deferred.mark_for_deferred_indexing

        self._run([(5, 1, deferred.OP_CACHE, "default")])

        self.assertIs(deferred.mark_for_deferred_indexing, original)

    def test_a_failed_flush_does_not_fail_the_job(self):
        with patch.object(deferred, "_flush", side_effect=RuntimeError("redis down")):
            with batched_search_indexing():
                deferred.mark_for_deferred_indexing(5, 1, deferred.OP_CACHE, "default")

    def test_nested_use_flushes_once_at_the_outer_exit(self):
        flushed = []
        with patch.object(
            deferred, "_flush", side_effect=lambda batch, alias: flushed.append(batch)
        ):
            with batched_search_indexing():
                with batched_search_indexing():
                    deferred.mark_for_deferred_indexing(
                        5, 1, deferred.OP_CACHE, "default"
                    )
                self.assertEqual(flushed, [])
        self.assertEqual(len(flushed), 1)

    def test_a_netbox_without_the_internals_is_left_alone(self):
        with patch.dict("sys.modules", {"netbox.search.deferred": None}):
            with batched_search_indexing():
                pass
        self.assertFalse(search_batching._active)


class RealSavepointsTest(TestCase):
    """The reported shape: many saves, each in its own savepoint, inside one job."""

    def test_many_savepoint_saves_queue_one_batch(self):
        from dcim.models import Site

        flushed = []
        with patch.object(
            deferred, "_flush", side_effect=lambda batch, alias: flushed.append(batch)
        ):
            with batched_search_indexing():
                for index in range(30):
                    with transaction.atomic():
                        Site.objects.create(name=f"bs-{index}", slug=f"bs-{index}")

        self.assertEqual(len(flushed), 1)
        self.assertGreaterEqual(len(flushed[0]), 30)


class JobsAreWrappedTest(SimpleTestCase):
    def test_the_sync_and_merge_jobs_collect_search_work(self):
        from forward_netbox import jobs

        self.assertTrue(hasattr(jobs.sync_forwardsync, "__wrapped__"))
        self.assertTrue(hasattr(jobs.merge_forwardingestion, "__wrapped__"))
        # Still importable and enqueueable under their own names.
        self.assertEqual(jobs.sync_forwardsync.__name__, "sync_forwardsync")
        self.assertEqual(jobs.sync_forwardsync.__module__, "forward_netbox.jobs")

    def test_the_decorator_runs_the_body_inside_the_collection(self):
        from forward_netbox import jobs

        seen = {}

        @jobs._batch_search_indexing
        def body():
            seen["active"] = search_batching._active
            return "done"

        self.assertEqual(body(), "done")
        self.assertTrue(seen["active"])
        self.assertFalse(search_batching._active)

    def test_the_shared_job_runner_collects_search_work(self):
        import inspect

        from forward_netbox import jobs

        self.assertIn(
            "batched_search_indexing", inspect.getsource(jobs.ForwardJobRunner.handle)
        )
