# Fix: endpoint import on tag-scoped syncs + tag merge name collision (2.4.4)

## Goal

Make opt-in SNMP-endpoint import (`sync_endpoints`) actually work on syncs that
use a device-tag include scope, and stop the merge-phase
`Tag with this Name already exists` ingestion issues.

## Constraints

- No schema or migration changes; drop-in from 2.4.3.
- Endpoint import stays additive: a failed endpoint probe must never fail a
  scoped sync.
- Local scope filtering and out-of-scope prune must not delete imported
  endpoints on later scoped syncs.
- Never commit the validation-org network id / token; validate live via the local source.

## Touched Surfaces

- `forward_netbox/utilities/query_fetch_execution.py`
  (`_resolve_scoped_tag_scope`, new `_resolve_scoped_endpoint_names`,
  `resolve_context` cache keys, `_context_artifact_descriptor` v3)
- `forward_netbox/queries/forward_devices.nqe` and
  `forward_devices_with_netbox_aliases.nqe` (endpoint branch include-scope
  removal; exclude tags kept)
- `forward_netbox/utilities/bulk_merge.py` (`_flush` extras.tag name/slug
  coalesce)
- Tests: `test_endpoints_import.py`, `test_bulk_merge.py`

## Approach

Two independent defects made tag-scoped endpoint import impossible:

1. Query-side: the endpoint union branch required endpoints to carry the
   device include tags. Endpoints rarely share device scoping tags (they scope
   the modeled-device universe), so every endpoint was filtered in Forward.
   The endpoint branch now applies exclude tags only.
2. Plugin-side: `scoped_device_names` was resolved from `network.devices`
   only, so the local scope filter (`_apply_device_tag_scope`) dropped every
   endpoint row unconditionally — and prune-out-of-scope would emit them as
   deletes. With `sync_endpoints` on, a second probe over `network.endpoints`
   (exclude tags honored, warn-and-skip on failure) unions endpoint names into
   the scoped set, so the local filter and prune keep endpoint rows. The
   context memo key and the shared context artifact (v3) include the toggle so
   a cached scope without endpoint names is never reused.

Merge fix: while a merge applies device UPDATEs (ordered before CREATEs),
netbox_branching sets device tags by name, get_or_creating the tag on main with
a new pk; the branch's tag CREATE then violated the unique name constraint and
surfaced as a ValidationError issue. `_flush` now treats a same-named or
same-slug main-side `extras.tag` as already merged (skip, counted as applied).

## Validation

Live against the validation-org demo network, reproducing the design partner's scenario
(include tags endpoints don't carry + `sync_endpoints` on): both device queries
still emit 355 Avocent/console endpoints with include params unused; the scope
union adds 688 endpoint names; `_apply_device_tag_scope` keeps an endpoint row
while dropping an out-of-scope device. Unit tests cover the scope union, the
exclude-tag probe, probe-failure softness, the NQE include-scope removal, and a
real branch-merge tag name collision (skips, no fallback, no issue). Full suite
+ lint + harness green.

## Rollback

Revert the branch — pure Python + query text + tests; no data migration. The
context artifact version bump only invalidates a transient cache.

## Decision Log

- Include scope intentionally does NOT gate endpoints: requiring shared tags
  silently broke the feature for every scoped source; exclude tags remain as
  the targeted safety valve. Documented in the query comments and README.
- Endpoint names are unioned into `scoped_device_names` rather than exempting
  endpoint rows in `_apply_device_tag_scope`, so prune and shard-key pushdown
  stay consistent without changing row shapes.
- The tag coalesce lives in the bulk `_flush` path (skip before build), not the
  per-object fallback, so the batch never hits the IntegrityError rollback.
- Endpoint-probe failure disables endpoint emission for the run (and skips the
  context-artifact save) rather than proceeding with an endpoint-less scope:
  otherwise the query would still emit endpoint rows, the local filter would
  drop them, and prune-out-of-scope would delete previously imported endpoints
  (adversarial-review finding).
- A skipped same-named tag create may carry different non-unique attrs
  (color/description); these converge on the next sync via the apply-time
  coalesce-by-name UPDATE — accepted over adding a merge-time write path.
