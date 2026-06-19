# Bulk-Apply Update Churn Fix + Device Scope Reconciliation Audit

## Goal

Address two more items from Partner's live CustomerOrg feedback:

1. "A lot of unnecessary updates" every sync. The bulk-ORM apply paths for
   `dcim.macaddress` (default-enabled), `dcim.interface`, and `ipam.ipaddress`
   re-wrote existing rows unconditionally, so every run PATCHed unchanged
   objects and reported them as updates.
2. NetBox holds more devices than the tag scope should match (2433 vs ~2196).
   Operators need a safe way to see why and what to do.

## Constraints

- No behavior change to created/changed rows — only skip writes for genuinely
  unchanged rows. Final DB state must stay identical (adapter parity preserved).
- The reconciliation audit is strictly read-only.
- FK comparisons must not trigger per-row lazy fetches (compare by id).
- No customer data, credentials, or network IDs in repo/tests.

## Touched Surfaces

- `forward_netbox/utilities/apply_engine_bulk.py` —
  `bulk_orm_apply_macaddress`, `bulk_orm_apply_interface`,
  `bulk_orm_apply_ipaddress` now compare before update and report `unchanged`;
  add `_interface_field_differs` helper.
- `forward_netbox/management/commands/forward_device_scope_reconciliation_audit.py`
  — new read-only audit command.
- `forward_netbox/tests/test_bulk_adapter_parity.py` — no-write-on-reapply
  regression tests for all three models.
- `forward_netbox/tests/test_device_scope_reconciliation_audit_command.py` —
  new command tests.
- `docs/01_User_Guide/troubleshooting.md` — "NetBox Has More Devices Than
  Expected" section.

## Approach

Churn: each bulk apply splits into create / changed / unchanged. Existing rows
are compared field-by-field (interface via `_interface_field_differs`, relations
by `<field>_id`); only changed rows enter the `bulk_update` bucket, and the
`unchanged` outcome is counted instead of `applied`. `bulk_update` is still
guarded by a non-empty bucket, so a fully-unchanged batch issues zero writes.

Reconciliation: resolve the sync's tag scope, run one read-only NQE over scoped
devices returning name + completed flag, and compare against NetBox device
names. Report in-scope/completed, tagged-but-backfilled, out-of-scope (stale),
and missing, with capped samples and a prune remediation. `--fail-on-drift`
exits non-zero for monitoring.

## Validation

- `forward_netbox.tests.test_bulk_adapter_parity` (parity unchanged; reapply
  issues no `bulk_update` and reports `unchanged`).
- `forward_netbox.tests.test_device_scope_reconciliation_audit_command`.
- Full `forward_netbox.tests` suite.
- `invoke harness-check`, lint.

## Rollback

Revert `apply_engine_bulk.py` to unconditional updates and delete the audit
command + tests. No schema, data, or migration impact.

## Decision Log

- Compare-before-write rather than relying on Branching/ORM to no-op: the bulk
  paths write via `bulk_update`, which always issues SQL, so the guard must live
  in the plugin.
- FK comparison by `<field>_id` to avoid an N+1 of lazy relation fetches purely
  for change detection.
- `ipam.ipaddress` already gated its update bucket on `changed`; only its
  statistics counter was corrected (it reported unchanged rows as applied),
  which is exactly the number operators read as "unnecessary updates."
- Reconciliation shipped as an on-demand command, not automatic pruning:
  deleting devices is destructive, so the tool surfaces the set and points at
  the existing `device_tag_prune_out_of_scope` opt-in.
