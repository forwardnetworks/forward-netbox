# Include Tags That Match No Device Stop the Run; Used Catalogue Rows Are Kept

## Goal

A sync whose include tags match no device in the selected Forward snapshot stops
before fetching anything, with a message naming the cause, instead of deleting.
Independently, the sync never deletes a platform, manufacturer, device role or
device type that a NetBox row still references.

Found live on the customer-shaped stack (3.0.13 behaviour, not new in 3.0.14):
with the include tag set to a name no device carries, every row was filtered out
locally and every model whose durable state compares against the previous run
read its whole table as gone. One run deleted all 13 platforms (the device
`platform` foreign key is SET_NULL, so the field was blanked on 3,572 devices),
58 unused device types, one role, and all 23 OSPF areas with their 2,931 OSPF
interfaces. Devices, interfaces, addresses, BGP peers and cables were untouched
(their models reset on the parameter change instead).

## Constraints

- Fail closed: an empty scope is never a reason to delete.
- A catalogue row Forward stops reporting may still be in use by devices out of
  scope, uncovered devices, or rows the operator made; blanking a field on them
  is never the sync's call.

## Touched Surfaces

- `forward_netbox/utilities/query_fetch_execution.py` `resolve_context`: raises
  `ForwardQueryError` when include tags are set and the scoped device set is
  empty.
- `forward_netbox/utilities/sync_primitives.py`: `SHARED_CATALOG_MODELS`,
  `_rows_a_delete_would_blank`; `delete_by_coalesce` turns a delete that would
  SET_NULL another row into the same protected-dependency skip PROTECT produces.
- Tests: `test_sync.py` (`test_include_tags_that_match_no_device_stop_the_run`),
  `test_catalog_delete_keeps_used_rows.py`.

## Approach

See Touched Surfaces. The skip reuses `protected_delete_skip`, so it is counted
and worded like every other "still referenced" skip.

## Validation

- `invoke test-isolated` for the two test modules plus test_sync,
  test_protected_delete_is_a_skip, test_endpoints_import, test_scope_matched_tags
  (382 tests, OK on the test host).
- Live: repeat the empty-tag run on the customer-shaped stack after restoring it;
  the run must fail with the message and change nothing.

## Rollback

Revert the commit.

## Decision Log

- Rejected: keeping the empty-scope run and only fixing the catalogue delete.
  OSPF areas are deletable by design, and their cascade took the OSPF interfaces;
  an empty scope says nothing about what Forward has, so nothing should run.
