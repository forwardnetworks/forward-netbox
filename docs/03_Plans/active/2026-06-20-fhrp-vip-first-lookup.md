# FHRP VIP-First Lookup

## Goal

Harden `apply_ipam_fhrpgroup` with a VIP-first group lookup so that name drift
(e.g., VRF data newly present in NQE changes the canonical group name) does not
cause a spurious create+delete cycle.

## Constraints

- No customer identifiers in the diff.
- VIP-first lookup must not break the common case (no name drift).
- The regression test must target the `_split_diff_rows` state-flip dedup that
  was the primary fix, not the VRF-change scenario the old test wrongly modelled.

## Bundled changes

1. **`_find_fhrp_group_by_vip`** — helper that resolves the FHRPGroup currently
   owning a given VIP, scoped by protocol and group_id to avoid false matches.
2. **`apply_ipam_fhrpgroup` VIP-first path** — try `_find_fhrp_group_by_vip`
   before `_coalesce_update_or_create`; migrate group name if stale.
3. **`test_fhrp_state_flip_does_not_churn_group`** — replaces the failing
   VRF-change test with an end-to-end assertion that state-flip diffs produce
   zero delete_rows and leave the group intact.

## Touched Surfaces

- `forward_netbox/utilities/sync_ipam.py` — new helper + modified apply path.
- `forward_netbox/tests/test_sync.py` — replaced test.

## Approach

Call `_find_fhrp_group_by_vip(runner, host_ip, vrf, protocol, group_id)` at the
top of `apply_ipam_fhrpgroup`. If it returns a group, skip the coalesce lookup
entirely and migrate the name if needed. If it returns None, proceed with the
existing `_coalesce_update_or_create` path unchanged.

## Validation

- `invoke test` — 1177 tests, 0 failures, 26 skipped.
- Manual desk-check: existing FHRP integration tests pass; new regression test
  primes `_model_coalesce_fields` and verifies the dedup fires correctly.

## Rollback

Revert this commit (`2dae73a`). The group-level dedup and shard co-location
(primary fixes for Partner's 13/13 churn) remain intact.

## Decision Log

- **VIP-first instead of broader coalesce fallback** — coalescing by (protocol,
  group_id, address) without VRF could match across VRF boundaries. VIP-first
  is narrower and verifies that the group actually owns the VIP at the expected
  address.
