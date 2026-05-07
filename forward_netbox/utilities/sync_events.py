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
