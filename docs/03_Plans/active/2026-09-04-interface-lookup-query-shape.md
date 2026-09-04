# The interface lookup that could not use an index

## Goal

Cut the SQL cost of resolving interfaces during a comparison. A customer's
drift report spent **1010213 ms** over 648353 rows in 246591 queries, and named
its own worst offender: `dcim.macaddress at 297643 ms in 756 queries, 247134 ms
of it in SQL`. Four of those minutes were one lookup shape.

## Why

`bulk_orm_apply_macaddress` must resolve every row's interface before it can
classify anything. That lookup chunked the `(device name, interface name)`
PAIRS 500 at a time and built, per chunk, one `Q(device__name=..., name__in=[...])`
branch per device, OR'd together - joining `dcim_device` by **name**.

Postgres cannot use an index for an OR-tree of that shape, so every chunk
scanned `dcim_interface` whole. On the estate that reported it that is 360,771
rows, and 124k MAC rows is roughly 248 chunks. The scan is the 247 seconds.

The join was unnecessary as well as expensive: both call sites resolve
`devices_by_name` immediately above, so the device ids were already in hand.

## Constraints

- **The result must be identical.** Chunking by device means the name filter is
  a superset across the chunk, so an exact pair check is not optional - without
  it an interface would match on the wrong device, which is worse than slow.
- **Devices absent from NetBox stay absent.** A pair whose device did not
  resolve must not silently match anything.
- **No behaviour change beyond cost.** This is the comparison and the apply
  reading the same rows they always did.

## Touched Surfaces

- `forward_netbox/utilities/apply_engine_bulk.py` -
  `_interfaces_by_device_and_name` replaces `_device_scoped_name_query` at both
  call sites, and the dead builder is removed
- `forward_netbox/tests/test_interface_lookup_query_shape.py` (new)

## Approach

Chunk by **device** rather than by pair. Each chunk is one query filtering
`device_id IN (...)` with the union of the names wanted across those devices,
which is a predicate an index can serve. The exact pair match then happens in
Python, where it costs nothing.

`.order_by()` clears NetBox's default Interface ordering - device name, then
the naturalized `_name` under a collation. The rows go straight into a dict, so
nothing reads the order, but the database was paying for the sort on every
chunk.

`sync_primitives` has its own same-named helper that is already id-based and is
deliberately untouched: it serves the dependency-lookup priming path, which was
tuned separately, and widening this change into it would be changing something
this evidence says nothing about.

## Validation

- The query is asserted on its **predicate**, not its text: `device_id IN` is
  present, a device name is not, and there is no `OR` - the three properties
  that decide whether an index can be used.
- Query count is asserted to scale with devices rather than pairs: 24 pairs
  across 6 devices is one query.
- Correctness is pinned where the superset could bite: every device in the
  fixture has an `Ethernet0`, and asking for one device's must return only
  that one.
- A device outside the resolved set is skipped; an absent name is simply
  absent; no pairs asks nothing.
- Full Django suite: 2617 tests.

## Rollback

Revert. The helper is self-contained and the call sites are two lines each.

## Decision Log

- **Chunk by device, not by pair.** Pair chunking was there to avoid a
  device/interface Cartesian product across batches; grouping by device solves
  the same problem without an OR-tree, because a device's interfaces are the
  natural unit of the lookup.
- **`select_related("device")` is kept.** It adds a join, but on a primary key
  and only over rows already filtered. Callers read `interface.device`, and
  removing it would trade a cheap join for a per-row query.
- **The predicate is asserted, not the SQL string.** Pinning the whole
  statement would break on any unrelated NetBox field addition and teach the
  next reader to delete the test.

## Open

- **This does not close the macaddress cost.** It removes the scan; whether the
  remaining time is the MAC batch lookup, the classification, or something
  else needs the per-model breakdown from a run that has this fix.
- **246591 queries overall is still the shape to look at next.** Adapter models
  compare one row at a time, so a model with 12310 rows issues 12310 preview
  applies. That is a design question, not a bug, and it is untouched here.
