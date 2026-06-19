# Device Tag Scope for Network-Wide Models

## Goal

Make network-wide object types respect the sync's device tag filter, so a sync
scoped to (e.g.) `Prod_Core` does not import VLANs/VRFs that belong only to
out-of-scope devices.

## Constraints

- Inclusive: keep an object if any in-scope (tagged) device uses it.
- Source-side (NQE) scoping, matching the existing prefix/locations pattern.
- No customer data in repo/tests.

## Findings (investigation)

- `device_tag_filter_mode` defaults to `local`; `get_query_parameters` only emits
  `device_tag_*` params, but the queries that declare those params receive them in
  all modes (`_apply_context_tag_parameters`, query_fetch_execution.py).
- `ipam.prefix` and `dcim.site` (forward_prefixes_*, forward_locations) already
  declare the `device_tag_*` params and self-scope to tagged devices.
- `ipam.vlan` (forward_vlans) and `ipam.vrf` (forward_vrfs) declared only
  `forward_netbox_shard_keys` — no device-tag filter — and their rows carry no
  device field, so `_apply_device_tag_scope` keeps every row. They leaked.
- Prefix breadth: prefixes derived from in-scope devices' routing/forwarding
  tables are network-wide (a tagged router knows remote routes). On ORG, routing-
  derived Prod_Core prefixes = 91,859 vs connected-subnet = 26,223. DECISION:
  switch prefix derivation to connected interface subnets (the networks devices
  actually host) as the default — true IPAM ownership, ~3.5x smaller.

## Touched Surfaces

- `forward_netbox/queries/forward_vrfs.nqe` — add `device_tag_include_tags`,
  `device_tag_include_match`, `device_tag_exclude_tags` params + include/exclude
  where-clauses on `device.tagNames`.
- `forward_netbox/queries/forward_vlans.nqe` — same, threaded through the
  `candidateRows` helper and the `@query` `f`.
- `forward_netbox/utilities/query_registry.py` — seed the device-tag params for
  `forward_vlans`/`forward_vrfs` (added to the device-tag query-file allowlist).
- `forward_netbox/queries/forward_prefixes_ipv4.nqe` and
  `forward_prefixes_ipv6.nqe` — rederive prefixes from connected interface
  subnets (subinterface/bridge/tunnel/routed-VLAN L3 addresses), canonicalized to
  the network address, instead of routing-table entries. Keeps the device-tag
  scope. ORG: v4 routing 91,859 -> connected 26,223 scoped; v6 connected 49.

## Approach

Mirror the validated prefix/locations tag where-clauses. The query-registry spec
detection keys off the literal `device_tag_include_tags` token in the source, so
once present the spec gains the params and the runtime injects the live tag values
from context.

## Validation

- NQE linter clean (no errors) on both queries.
- Live ORG: `forward_vrfs` unscoped=425 vs scoped(Prod_Core)=288 — the tag filter
  drops out-of-scope VRFs. (forward_vlans uses the identical pattern but imports
  the org `netbox_utilities` module so it cannot be run standalone ad-hoc; the
  where-clauses are identical to the validated VRF/prefix queries.)
- Full plugin suite; local CI mirror.

## Rollout note

These are org-published queries with a changed signature (new params). The org
NQE maps must be republished (forward_validation_org_query_audit --repair
--overwrite) for query_id-mode syncs to pick up the scoped versions.

## Migration heads-up (prefix prune on first sync)

Switching prefixes from routing-derived to connected-subnet derivation makes the
first post-upgrade sync a large one-time PRUNE: prefixes that were routing-only
(learned/remote/default routes) and are not connected subnets become deletes.
ORG scale: ~91,859 -> ~26,223 under the Prod_Core tag, i.e. ~65k prefix deletes on
the first run; an untagged/full sync prunes more. In Branching mode these deletes
stage in a review branch first (review before merge); fast-bootstrap applies
directly. Anything referencing the pruned prefixes (IP assignments, custom data,
parent/child) is affected. Steady state after the one-time prune is clean.
Operators should review the first 1.6.1 sync branch before merging.

## Rollback

Revert the two `.nqe` files to the shard-key-only signatures and republish.

## Decision Log

- Scope VLANs/VRFs at the NQE source (option a) rather than a Python row filter:
  smaller fetches and matches the prefix/locations pattern already in the tree.
- Prefix/site left as-is: both already self-scope; prefix routing-table breadth
  is inherent and a connected-subnet derivation is a separate product decision.
