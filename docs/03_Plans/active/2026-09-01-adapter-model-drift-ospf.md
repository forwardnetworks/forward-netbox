# Measure drift for the OSPF models

## Goal

Slice eight of eight, and the last of the adapter-only drift comparison:
`netbox_routing.ospfinstance`, `netbox_routing.ospfarea` and
`netbox_routing.ospfinterface`.

## Why

Unchanged, and this is the slice that ends it: every adapter-only model named
in #206 now reports a measurement rather than an upper bound.

## Constraints

- Reuse the apply's own resolution and comparisons.
- Write nothing.
- A row the apply refuses must not read as drift.
- **A parent that is separately measured must not be counted twice.**

## Touched Surfaces

- `forward_netbox/exceptions.py` - `ForwardQueryError` accepts the structured
  keywords two callers already passed it
- `forward_netbox/utilities/sync_routing_impl.py` - `preview` threaded through
  the instance chain; the three apply functions classify
- `forward_netbox/utilities/drift_comparison.py` - three chain delegations and
  the dispatch entries
- `forward_netbox/tests/test_ospf_drift_comparison.py` (new)

## Approach

### The audit is short, and that is the finding

Every write in these three chains is already behind a `runner.` call the
preview overrides - `_upsert_values_from_defaults` for all three models,
`_ensure_vrf` beneath the instance, `_lookup_interface` beneath the interface.
There is no direct save here, unlike the BGP neighbour address (slice seven),
the FHRP virtual IP (slice four) or `Cable.save()` (slice two).

`lookup_routing_interface_name` falls back to a raw `Interface.objects.filter`
and then calls `remember_lookup_object`, which was checked: it only writes into
the runner's own memoisation dicts, every one of which `PreviewRunner` seeds.

### The verdict rule is the OPPOSITE of slice seven's

Slice seven takes the strongest outcome across every object a row touches,
because a BGP peer's `BGPRouter` and `BGPScope` have no Forward query of their
own - the peer is the only place their drift can ever be reported.

Every OSPF parent is a **separately measured model**. `ospfinstance` and
`ospfarea` each have their own query in `query_registry.py` and their own row
set. So folding a parent's create into the interface's verdict would count one
object twice - once under the model that owns it, and again under this one -
and inflate total drift by exactly the number of shared parents.

`preview_leaf_outcome` therefore classifies from the leaf's own upsert, the way
the flat DLM rows do. The two rules sit next to each other in
`sync_routing_impl.py` with the reason on each, because the difference is not
obvious from either call site and picking the wrong one is silent in both
directions.

### An absent VRF, again

`ensure_ospf_instance` coalesces on `("device", "vrf", "process_id")`, so it has
the same collision slice seven fixed for BGP scopes: an unresolved VRF left
`vrf=None` on the lookup and matched the device's GLOBAL instance, reporting an
instance the apply would create as already present. It uses the same
`routing_vrf` guard and returns a create.

### `ForwardQueryError` took no keyword arguments

Found by the preview, but the bug is in the APPLY, and it is the more serious
of the two findings here.

Two callers build it with `model_string=`, `context=` and `data=`:

- `ensure_bgp_address_family` - an address family NetBox does not offer
- `ensure_ospf_instance` - a row with no `router_id`

`ForwardQueryError` subclasses `ForwardSyncError`, which defines no `__init__`,
so **both raised `TypeError` instead of the exception they name**.
`apply_model_rows` catches `ForwardQueryError` per row, records the issue and
continues (`sync_reporting.py:589`); it catches no `TypeError` anywhere. So one
malformed OSPF row, or one unsupported BGP address family, aborted the apply
for its entire model rather than being recorded and skipped - and the recorded
failure would have named `TypeError`, which says nothing about the row.

Fixed by giving `ForwardQueryError` the same structured keywords
`ForwardDataError` has. Deliberately NOT reparented under `ForwardDataError`:
several `except ForwardDataError` sites would silently start catching query
failures, and nothing here needs that.

## Validation

`forward_netbox/tests/test_ospf_drift_comparison.py`, 19 tests: the firewall,
the classification for all three models, the absent-VRF collision, the two
double-counting cases, the rejection paths, and three on the exception
signature including one that drives the real OSPF raise site.

## Rollback

Revert. The models return to the workload upper bound; nothing else reads the
preview paths this slice added.

## Decision Log

- The verdict rule is stated at the call site in `sync_routing_impl.py`,
  because the two rules are opposite and getting it backwards is silent in
  both directions. See the rule of thumb in the Approach.

## Limits

**Every adapter-only model NAMED in #206 is now measured. That is not the same
as every model.** A static sweep of `query_registry.py` against
`_ADAPTER_COMPARISONS` and the bulk dispatcher leaves ten fetched models still
reporting an upper bound:

| model(s) | why |
| --- | --- |
| `netbox_cisco_aci.*` (8) | adapter-only, never in the eight-slice scope - #206 predates the ACI maps |
| `netbox_dlm.inventoryitemsoftware` | deferred in slice six, pending an audit of `_lookup_inventory_item` |
| `dcim.virtualchassis` | deliberate: it creates rows and THEN assigns devices to them |

So a deployment with netbox-cisco-aci or netbox-dlm still reports a PARTIAL
measurement, which the report states as "N of M models" rather than implying
whole-estate coverage. `in_sync` is answerable end to end only for a deployment
running neither optional plugin.

Closing ACI is a ninth slice and is the largest single block of what remains;
it was not in this stack's scope and is not claimed by it.
- The `ForwardQueryError` fix is not covered by a test that exercises the
  BGP address-family raise site through a real apply; only the OSPF one is
  driven end to end.
