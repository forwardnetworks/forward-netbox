# Event Clearing on Commit

## Goal

Make batch event flushing safe when the sync runner is already inside a transaction, so large prefix ingestions do not try to toggle autocommit mid-flight.

## Constraints

- Preserve event delivery semantics as much as possible.
- Keep the change inside the existing sync/event-clearer path.
- Avoid introducing a new event pipeline.

## Touched Surfaces

- `forward_netbox/utilities/sync.py`
- `forward_netbox/tests/test_sync.py`

## Approach

1. Defer event flushing through `transaction.on_commit(...)` instead of flushing immediately inside the row loop.
2. Keep the existing queue clear behavior so the batch loop still releases accumulated events.
3. Add a regression test that confirms the clearer schedules flush work through `on_commit`.

## Decision Log

- Chose `transaction.on_commit(...)` over immediate flushes so the sync runner does not try to change autocommit during an active transaction.
- Rejected suppressing event flushing entirely because the event pipeline should still process committed changes.

## Validation

- `invoke lint`
- `invoke test`
- `invoke ci`

## Rollback

- Revert the `EventsClearer.clear()` change and its regression test if the event flush timing causes a regression elsewhere.
