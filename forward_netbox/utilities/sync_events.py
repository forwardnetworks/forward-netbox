from core.signals import clear_events
from django.db import transaction
from extras.events import flush_events
from netbox.context import events_queue


def reset_delete_signal_memory():
    """Forget which objects NetBox has already seen deleted in this thread.

    NetBox's pre-delete handler skips an object whose (type, pk) it has already
    processed, so a cascade is logged once. It clears that memory only when a web
    request ends. A worker job never ends a request, and the branch shares main's
    ids, so a delete staged in the branch makes the merge's delete of the same
    object look already handled and writes no change-log row in main (133 routing
    deletes on one run). Call this where one phase ends and the next begins.
    """
    try:
        from core.signals import _signals_received
    except ImportError:  # a NetBox that moved it: the merge still works, unlogged
        return
    _signals_received.pre_delete = set()


class EventsClearer:
    def __init__(self, threshold=100):
        self.threshold = threshold
        self.counter = 0

    def increment(self):
        self.counter += 1
        if self.counter >= self.threshold:
            self.clear()

    def clear(self):
        queued_events = events_queue.get() or {}
        if events := list(queued_events.values()):
            transaction.on_commit(lambda: flush_events(events))
        clear_events.send(sender=None)
        self.counter = 0

    def snapshot(self):
        """Copy of the current queued-events map.

        The events_queue contextvar is NOT transactional, so a per-row save()
        that then rolls back (isolated-row savepoint) leaves its events in the
        queue to be flushed by the next clear(). Snapshot before an isolated
        row and restore() on failure to drop exactly that row's events. Shallow
        copy is safe: receivers only add new keys, never mutate existing ones.
        """
        return dict(events_queue.get() or {})

    def restore(self, snapshot):
        events_queue.set(dict(snapshot))
