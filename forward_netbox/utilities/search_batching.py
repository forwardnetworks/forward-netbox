# One search-index job per few thousand committed objects, not one per saved row.
#
# NetBox 4.7 defers search indexing past the transaction: it collects the objects
# a transaction touched and, when it commits, `netbox.search.deferred._flush`
# enqueues a "Search cache update" job. It keeps one collection per savepoint
# scope, so the plugin's per-row savepoints (every row of a sync or merge is
# applied in its own) made thousands of jobs: a customer's sync, post-sync step
# and config backup all queued behind a flood of them, and a settings save timed
# out at the gateway.
#
# During a plugin job this replaces `_flush`, which NetBox calls only for work
# that has COMMITTED (from the on-commit callback, or inline in autocommit). A
# rolled-back savepoint's work never reaches it, exactly as before. The committed
# batches are merged per database alias, the later operation winning, and handed
# to the real `_flush` in bounded chunks: whenever one alias reaches the chunk
# size, and once more when the job ends. A branch schema dropped before the end
# (an auto-merge removes it) is skipped rather than enqueued against nothing. If
# the NetBox internals this reads are not present, nothing is patched.
import logging
from contextlib import contextmanager
from contextvars import ContextVar

from django.db import DEFAULT_DB_ALIAS

logger = logging.getLogger("forward_netbox.search_batching")

# Entries handed to NetBox per flush: bounds each job's payload and this buffer.
FLUSH_CHUNK_SIZE = 5000

_active = False

# The cache `job_query_cache` installed, so code inside NetBox's `event_tracking`
# (which installs its own empty one) can go back to it.
_job_cache = ContextVar("forward_netbox_job_query_cache", default=None)


def _alias_usable(alias):
    """Whether ``alias`` still reaches a schema holding the search cache table."""
    if alias == DEFAULT_DB_ALIAS:
        return True
    # A branch alias's search_path is "<schema>,<main schema>", and Postgres
    # skips a missing schema in it, so a query on a dropped branch's alias
    # silently reads main's table. Ask the catalogue for the schema itself.
    schema = alias.removeprefix("schema_") if alias.startswith("schema_") else None
    try:
        from django.db import connections

        with connections[DEFAULT_DB_ALIAS].cursor() as cursor:
            if schema is not None:
                cursor.execute(
                    "SELECT 1 FROM pg_namespace WHERE nspname = %s", [schema]
                )
                return cursor.fetchone() is not None
        from extras.models import CachedValue

        CachedValue.objects.using(alias).exists()
        return True
    except Exception:  # noqa: BLE001 - a gone alias
        return False


class _Buffer:
    def __init__(self, real_flush):
        self.real_flush = real_flush
        self.by_alias = {}
        self.failed_aliases = set()

    def add(self, batch, using):
        alias = using or DEFAULT_DB_ALIAS
        merged = self.by_alias.setdefault(alias, {})
        # Committed in order, so a later commit's operation is the current one.
        merged.update(batch or {})
        if len(merged) >= FLUSH_CHUNK_SIZE:
            self.flush_alias(alias)

    def flush_alias(self, alias):
        merged = self.by_alias.pop(alias, None)
        if not merged or alias in self.failed_aliases:
            return
        if not _alias_usable(alias):
            logger.info(
                "Search index: %s entries for database alias %s skipped; its "
                "schema is gone (a merged branch).",
                len(merged),
                alias,
            )
            return
        try:
            self.real_flush(merged, alias)
        except Exception:  # noqa: BLE001 - indexing must never fail a job
            self.failed_aliases.add(alias)
            logger.exception(
                "Search index batch for alias %s could not be queued", alias
            )

    def flush_all(self):
        for alias in list(self.by_alias):
            self.flush_alias(alias)


@contextmanager
def batched_search_indexing():
    """Merge NetBox's committed search-index work for the enclosed job."""
    global _active
    try:
        from netbox.search import deferred

        real_flush = deferred._flush
    except (ImportError, AttributeError):
        yield
        return
    if _active:  # nested: the outer one flushes
        yield
        return

    buffer = _Buffer(real_flush)

    def collect(batch, using, *args, **kwargs):
        if args or kwargs:  # a signature NetBox changed: do what it asked
            return real_flush(batch, using, *args, **kwargs)
        buffer.add(batch, using)

    deferred._flush = collect
    _active = True
    try:
        yield
    finally:
        _active = False
        deferred._flush = real_flush
        buffer.flush_all()


@contextmanager
def job_query_cache():
    """Give a plugin job the per-run lookup cache NetBox gives its own jobs.

    NetBox caches the object-type and custom-field lookups it makes for every
    saved object in `netbox.context.query_cache`, which its `event_tracking` sets
    for a request "and the whole of a script or job run". The sync's staging runs
    outside it, so every row queried both again: about 60% of the database's busy
    time while staging a large estate. Both are definitions, not data the sync
    changes. A cache already set (a caller inside `event_tracking`) is left alone.
    """
    try:
        from collections import defaultdict

        from netbox.context import query_cache
    except ImportError:
        yield
        return
    if query_cache.get() is not None:
        yield
        return
    cache = defaultdict(dict)
    token = query_cache.set(cache)
    job_token = _job_cache.set(cache)
    try:
        yield
    finally:
        _job_cache.reset(job_token)
        query_cache.reset(token)


@contextmanager
def keep_job_query_cache():
    """Inside NetBox's `event_tracking`, keep using the job's lookup cache.

    `event_tracking` installs a fresh, empty query cache each time it is
    entered. The merge enters it once per row it applies on its own, so the
    job's cache was replaced by an empty one for every such row. Use this just
    inside it; with no job cache it does nothing."""
    try:
        from netbox.context import query_cache
    except ImportError:
        yield
        return
    outer = _job_cache.get()
    if outer is None:
        yield
        return
    token = query_cache.set(outer)
    try:
        yield
    finally:
        query_cache.reset(token)
