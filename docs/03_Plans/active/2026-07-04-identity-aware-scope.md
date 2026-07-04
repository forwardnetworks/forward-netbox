# Identity-aware (PK-tracked) scope reconciliation

**Date:** 2026-07-04

## Goal
Make device-scope reconciliation track device identity (PKs) end-to-end rather
than re-matching the non-unique device name at delete time.

## Constraints
- No change to which devices are pruned; only anchor the delete to explicit PKs.
- No schema change.

## Touched Surfaces
- `forward_netbox/utilities/scope_reconciliation.py` — `compute_scope_reconciliation`
  now resolves the out-of-scope device names to PKs at compute time and returns
  `_out_of_scope_pks`; `prune_orphan_devices` deletes those PKs directly (falling
  back to a name resolution for an older report shape).
- `forward_netbox/tests/test_device_scope_reconciliation_audit_command.py` —
  asserts `_out_of_scope_pks` resolves to the exact out-of-scope device PK.

## Approach
Resolve identity once, at the authoritative scope-compute step, and carry PKs
through to the delete — removing the fragile delete-time name re-match.

## Known limitation (documented, deliberate)
Scope MEMBERSHIP is still name-keyed: a Forward-scoped name conservatively protects
every NetBox device with that name. Distinguishing two same-named devices in
different sites requires a Forward location → NetBox site mapping, which is a
separate, larger change and is noted inline in the code.

## Validation
Scope + audit-command tests (9) pass; new `_out_of_scope_pks` test; lint/harness.

## Rollback
Revert; behavior-preserving (same device set), no schema impact.

## Decision Log
- Resolve PKs in `compute` (not only in `prune`) so any consumer of the report has
  the identity-resolved set, and the prune needs no second query.

## Bundled changes
- Scope reconciliation tracks device PKs end-to-end; prune deletes by PK.
