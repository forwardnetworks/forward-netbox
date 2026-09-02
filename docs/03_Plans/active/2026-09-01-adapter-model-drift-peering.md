# Measure drift for the peering models

## Goal

Slice seven of the adapter-only drift comparison: `netbox_routing.bgppeer`,
`netbox_routing.bgpaddressfamily`, `netbox_routing.bgppeeraddressfamily` and
`netbox_peering_manager.peeringsession`.

## Why

Unchanged. Every adapter-only model reports an upper bound, so `in_sync` stays
unanswerable while any of the eight is uncompared. These were left until last
because one Forward row means more persisted objects here than anywhere else.

## Constraints

- Reuse the apply's own resolution and comparisons.
- Write nothing - and a single BGP peer row writes in six places.
- A row the apply refuses must not read as drift.

## Touched Surfaces

- `forward_netbox/utilities/sync_routing_impl.py` - `preview` threaded through
  the peer chain; the four apply functions classify
- `forward_netbox/utilities/drift_comparison.py` - `_ensure_asn` and
  `_coalesce_upsert` overrides, the routing chain delegations, the per-row
  upsert record, `ForwardQueryError` in the row loop, the dispatch entries
- `forward_netbox/tests/test_peering_drift_comparison.py` (new)

## Approach

### What one row writes

A single `netbox_routing.bgppeer` row resolves - and the apply would persist -
six objects:

| object | reached via | shimmed by |
| --- | --- | --- |
| `ipam.ASN` x2 | `runner._ensure_asn` | new override (find-only) |
| `ipam.RIR` | `_ensure_forward_observed_rir`, under `_ensure_asn` | same override, never reached |
| `ipam.IPAddress` | `ensure_bgp_peer_ip` - **direct save** | `preview` argument |
| `netbox_routing.BGPRouter` | `runner._upsert_values_from_defaults` | already overridden |
| `netbox_routing.BGPScope` | `runner._coalesce_upsert` | new override |
| `netbox_routing.BGPPeer` | `runner._upsert_values_from_defaults` | already overridden |

Two of those were holes. `_ensure_asn` saves an `ASN` and calls
`_ensure_forward_observed_rir`, which upserts a `RIR` - the same
writes-behind-a-runner-call trap as `_ensure_vrf` and `_ensure_platform`, and
equally invisible to a grep for ORM calls in this module. `_coalesce_upsert` is
the THIRD upsert primitive: the model-string-carrying wrapper that resolves the
conflict policy before delegating to `coalesce_update_or_create`. The preview
overrode the delegate; the wrapper is defined separately on the runner and
would have been inherited whole.

`ensure_bgp_peer_ip` is the direct save, outside any `runner.` call - the same
shape as the FHRP virtual IP and cables - so it takes a `preview` argument
rather than a shim.

### Why the verdict is not the leaf row

`netbox_routing.bgprouter` and `netbox_routing.bgpscope` have contracts and
coalesce fields but **no Forward query of their own** (`query_registry.py`
registers six routing models; neither is among them). They exist only as
parents built while applying a peer.

So a router this run would rewrite is drift that NO model would report if the
peer's verdict came from `last_upsert_would_change` alone, the way the flat DLM
rows are classified. The peer would read `unchanged` while every run rewrote
its router - a confident zero, which is the one failure mode here with a real
consequence.

Hence `preview_routing_outcome` takes the strongest outcome across every upsert
the row performed, read from a per-row record the preview runner keeps
(`upsert_outcomes`, cleared by `begin_row` in the shared row loop).
`last_upsert_would_change` is untouched, so the flat paths that only ever
upsert one object classify exactly as before.

This is the same rule slice four applied to FHRP groups - "the row's verdict is
the strongest of the three" - generalised from three hand-written cases to
whatever the chain actually did.

### Absent parents short-circuit

Under preview the ASNs, the neighbour address, the router and the scope all
resolve rather than create, so any of them can come back `None`. Building the
peer's values against a missing parent is not merely pointless:

- `ensure_bgp_router` reads `local_asn.asn` for the router name, so a `None`
  ASN raises `AttributeError` - which no caller catches, so it would have
  killed the whole comparison rather than classifying one row.
- a scope coalesced on `router=None` matches whichever unrelated scope has no
  router.

So an absent parent returns `None` up the chain and the row is reported as a
create, which is also the honest answer: a peer cannot already exist against a
neighbour address NetBox does not have.

### An absent VRF must not resolve to the global one

Every coalesce set in these chains includes `vrf`. The real `_ensure_vrf`
CREATES a missing VRF and coalesces inside it; the preview override resolves
and returns `None` instead, which collides with the answer for a row that names
no VRF at all.

That collision is not cosmetic: a row whose VRF does not exist yet looked its
scope up on `vrf=None` and matched the unrelated GLOBAL scope, so a peer the
apply would create was reported as already present and unchanged. `routing_vrf`
now returns a distinct `VRF_ABSENT` under preview and the callers report a
create. The same fix covers the OSPF chain in slice eight, which shares this
helper.

### `ForwardQueryError` was escaping the row loop

The routing paths are the first to raise `ForwardQueryError` during
classification - an unparseable ASN, an empty `afi_safi`, an unsupported
address family. It is **not** a subclass of `ForwardDataError`, so the shared
row loop did not catch it, and one malformed row would have aborted the
comparison for every other row in the batch.

`apply_model_rows` catches it per row alongside `ForwardSearchError` and
`ForwardSyncDataError` (`sync_reporting.py:589`), records an issue and
continues. The loop now does the same and counts the row rejected, which is
what the apply does with that row. This was a latent defect in the earlier
slices, not one this slice introduced; no shipped adapter path raises it yet.

## Validation

`forward_netbox/tests/test_peering_drift_comparison.py`, 16 tests: the firewall
(no ASN, RIR, address, router, scope or peer created; a drifted peer not
rewritten), the classification (matching, absent, drifted), the absent-parent
short-circuits, the parent-drift case no other model would report, and the
rejection paths including one malformed row not taking the batch with it.

The converged fixture is built from the apply's own `bgp_peer_name` and
`bgp_peer_comments` rather than from literals, so it cannot quietly stop being
converged when either format changes.

## Rollback

Revert. The models return to the workload upper bound; nothing else reads the
preview paths this slice added.

## Decision Log

- The verdict rule is stated at the call site in `sync_routing_impl.py`,
  because the two rules are opposite and getting it backwards is silent in
  both directions. See the rule of thumb in the Approach.

## Limits

- `netbox_routing.bgprouter` and `netbox_routing.bgpscope` are still not
  models with counts of their own. They are reported through the peer that
  builds them, which is where their drift is actionable.
- Slice eight - the OSPF models - remains, and `in_sync` stays unanswerable
  until it lands.
- Two netbox-dlm sub-models (`inventoryitemsoftware`,
  `inventoryitemroleplatform`) remain uncompared from slice six.
