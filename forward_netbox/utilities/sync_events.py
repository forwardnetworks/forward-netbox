from core.signals import clear_events
from django.db import transaction
from extras.events import flush_events
from netbox.context import events_queue


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
