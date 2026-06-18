# Prefix Churn Investigation + Bulk Compare Hardening

## Goal

Investigate Partner's report that prefixes show "too many updates" each sync, and
harden the bulk apply comparison surfaced by that investigation.

## Findings

- **Production prefix path does not churn.** `ipam.prefix` is adapter-required,
  so it runs the adapter (`apply_ipam_prefix` → `coalesce_update_or_create`),
  which compares fields before writing and special-cases the `prefix` field
  (stored `IPNetwork` vs incoming string) in
  `sync_primitives._model_field_value_matches`. Re-applying an unchanged prefix
  is a no-op.
- **Pruning does not delete prefixes.** `_apply_device_tag_scope` keeps rows that
  carry no device names (prefix rows have none), so `device_tag_prune_out_of_scope`
  never turns prefixes into deletes. Prefix scoping happens in the tag-aware NQE
  itself, not via row pruning.
- Therefore the remaining "prefix updates" Partner sees are most likely true
  snapshot-to-snapshot changes or NetBox `_depth`/hierarchy recomputation, not a
  plugin churn bug. (Confirm with the branch diff: modified vs delete+create.)
- **Latent bulk-path gap (not currently reachable):** the bulk `simple_models`
  update loop compared with a naive `!=`, which mishandles typed fields and
  compares FKs by instance (forcing a lazy refetch). Harmless for today's default
  bulk set, but it would churn `ipam.prefix` if it were ever promoted.

## Constraints

- No behavior change for the reachable default bulk models (only correctness /
  fewer queries).
- Do not attempt to make `ipam.prefix` bulk-ready here — that is separate,
  deprioritized promotion work.

## Touched Surfaces

- `forward_netbox/utilities/apply_engine_bulk.py` — bulk `simple_models` update
  loop uses the shared `_model_field_value_matches` (relations by id, typed-field
  special cases) instead of `!=`.
- `forward_netbox/tests/test_apply_engine.py` — FK re-apply no-churn test.

## Approach

Reuse the adapter's `_model_field_value_matches` in the bulk update comparison so
both engines apply identical change detection. This compares relations by
`<field>_id` (no per-row lazy fetch) and special-cases `ipam.prefix` and
`netbox_routing.ospfinstance` typed fields.

## Validation

- `forward_netbox.tests.test_apply_engine`
  (`test_bulk_simple_models_reapply_fk_no_churn`; existing bulk create/update,
  targeted-validation, VRF-clobber tests).
- Full `forward_netbox.tests` suite.
- `invoke harness-check`, lint.

## Rollback

Revert the comparison line to `!=` and drop the test. No schema/data impact.

## Decision Log

- Keep the comparison change despite no reachable churn fix: it removes an FK
  N+1 (lazy refetch per existing row) on the default bulk FK models and unifies
  adapter/bulk change detection.
- ipam.prefix bulk promotion blockers (for the future, do NOT fix here): (1) the
  composite `(prefix, vrf)` bulk lookup key is `None` when vrf is null, so an
  existing null-VRF prefix is never matched and a duplicate create is attempted;
  (2) typed-field lookup-key normalization. These must be solved alongside IPAM
  hierarchy/`_depth` parity before ipam.prefix can leave the adapter.
