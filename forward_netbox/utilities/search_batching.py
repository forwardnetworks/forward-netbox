# One search-index job per sync, not one per saved row.
#
# NetBox 4.7 defers search indexing past the transaction: it collects the objects
# a transaction touched and, on commit, enqueues a "Search cache update" job. It
# keeps one collection per savepoint scope, so the plugin's per-row savepoints
# (every row of a sync or merge is applied in its own) make thousands of jobs: a
# customer's sync, post-sync step and config backup all queued behind a flood of
# them, and a settings save timed out at the gateway.
#
# During a plugin job this collects the same (object type, pk, operation) entries
# in one place, coalesced exactly as NetBox does (a delete wins over a create),
# and hands NetBox a few bounded batches when the job ends. NetBox's own apply
# skips an object that no longer exists, so a row a rolled-back savepoint dropped
# is harmless. If the NetBox internals this reads are not present, nothing is
# patched and NetBox indexes as it always did.
import logging
from contextlib import contextmanager

from django.db import DEFAULT_DB_ALIAS

logger = logging.getLogger("forward_netbox.search_batching")

# Entries handed to NetBox per flush: bounds each job's payload.
FLUSH_CHUNK_SIZE = 5000

_active = False


def _flush_in_chunks(collected, flush):
    for alias, batch in collected.items():
        items = list(batch.items())
        for start in range(0, len(items), FLUSH_CHUNK_SIZE):
            try:
                flush(dict(items[start : start + FLUSH_CHUNK_SIZE]), alias)
            except Exception:  # noqa: BLE001 - indexing must never fail a job
                logger.exception("Search index batch could not be queued")


@contextmanager
def batched_search_indexing():
    """Collect NetBox's deferred search-index work for the enclosed job."""
    global _active
    try:
        from netbox.search import deferred

        original = deferred.mark_for_deferred_indexing
        flush = deferred._flush
        op_remove = deferred.OP_REMOVE
    except (ImportError, AttributeError):
        yield
        return
    if _active:  # nested: the outer one flushes
        yield
        return

    collected = {}

    def collect(object_type_id, pk, op, using=None):
        batch = collected.setdefault(using or DEFAULT_DB_ALIAS, {})
        key = (object_type_id, pk)
        # A deletion supersedes a pending create or update, as in NetBox.
        if op == op_remove or batch.get(key) != op_remove:
            batch[key] = op

    deferred.mark_for_deferred_indexing = collect
    _active = True
    try:
        yield
    finally:
        _active = False
        deferred.mark_for_deferred_indexing = original
        _flush_in_chunks(collected, flush)
