# Ownership Conflict Reason, At The Formatter

## Goal

Make a failed ownership reconciliation name what refused it, on the path a
customer actually hits.

## Contract

- Every persisted failure message for `OwnershipConflictError` carries a slug.
- No device name or source key is ever persisted.
- An uncatalogued message records that it was unrecognised.

## Constraints

- Persisted diagnostics stay free of customer data.
- No import cycle: `diagnostics` must not import `ownership`.

## Touched Surfaces

- `forward_netbox/utilities/diagnostics.py` - the reason table and
  `safe_operation_failure`
- `forward_netbox/utilities/ownership.py` - re-export only
- `forward_netbox/jobs.py` - structured `conflict_reason` on both job paths
- `forward_netbox/tests/test_ownership_conflict_reason.py` - new
- This plan.

## Approach

2.6.11 added the reason to the scope-reconciliation *report* job. The job the
customer hit is `_reconcile_forward_device_scope_tags_work`, a different
function that formats the same sentence through its own error path, so the fix
passed its tests and changed nothing they saw. Their screenshot showed
`forward_ingestion_id` in the Data panel, which only `_overlay_job_data` sets -
the evidence that it was the other path was there from the start.

The reason now lives in `safe_operation_failure`, the single function all
sixteen failure paths format through, so no call site can miss it. It matches on
the exception's class name rather than importing `OwnershipConflictError`, which
keeps `diagnostics` free of a cycle; `ownership` re-exports the helper for
existing importers. Both job paths additionally record `conflict_reason` as a
field so it can be filtered on.

This is the third instance this session of the same defect: a helper written and
tested but not called at every site. The countermeasure here is placement -
putting it where every path already converges rather than remembering the sites.

## Validation

- New tests assert every known condition resolves, an unknown one records
  `unrecognized-ownership-conflict`, the device name and source key never reach
  the message, and the scope-tags job path records both the message and field.
- `forward_netbox.tests.test_ownership` 24 tests OK;
  `test_ownership_conflict_reason` 6 tests OK.

## Rollback

Revert. Messages return to naming the exception class alone.

## Decision Log

- 2026-08-01: Placed at the formatter, not the call sites. Enumerating call
  sites is what failed the first time.

## Open

- Ingestions that will not delete: four models PROTECT an ingestion through
  `ForwardIngestionProvenanceMixin` - `ForwardDeviceIdentity`,
  `ForwardDeviceTagClaim`, `ForwardVirtualParentClaim`,
  `ForwardOwnershipReconciliation`. Two undeletable ingestions is consistent
  with expected protection, but the refusal banner names the holding records and
  counts, and that text is what distinguishes expected protection from leaked
  ownership rows. Not diagnosed.
- SNMP endpoints not covered by an import Forward tag. Not investigated.
- The `primary-ip-reassignment-blocked` root cause remains deferred.
