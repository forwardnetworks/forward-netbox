# Hidden PROTECT Merge Prediction

## Goal

Predict and report merge deletes held by hidden `PROTECT`/`RESTRICT`
references before apply, without suppressing an authoritative Device delete
whose current-sync identity the plugin releases in the same merge transaction.

## Constraints

- Keep NetBox Branching in the ingestion and merge path.
- Preserve successful authoritative Device deletion and its ownership/relation
  writer serialization contracts.
- Exempt only an exact, documented plugin-owned relation and only rows the
  current merge sync can release; peer-sync or other provenance remains
  blocking.
- Do not add or reorder dependency edges. This change classifies impossible
  deletes as skips before the existing dependency sort, so it cannot create a
  dependency cycle.
- Keep `protecting_relations()` and ingestion-delete diagnostics unchanged.

## Touched Surfaces

- `forward_netbox/utilities/bulk_merge.py` - hidden relation prediction and the
  explicit self-resolved relation contract
- `forward_netbox/utilities/merge.py` - supply the current merge sync identity
  to prediction
- `forward_netbox/tests/test_bulk_merge.py` - hidden blocker and authoritative
  Device-delete regression coverage
- This plan

## Approach

1. Add a regression test in the production merge path: a Device delete held by
   a hidden `ForwardDeviceTagClaim.device` reference must be skipped and named
   before `ObjectChange.apply()` can fail it. Add a peer-sync identity case to
   prove that rows of the allowlisted model remain blocking outside the current
   merge sync.
2. Change `protecting_reference_blocked_deletes()` to enumerate
   `protecting_relations()`, including hidden relations.
3. Define one immutable relation signature for plugin-resolved protection:
   `dcim.Device <- ForwardDeviceIdentity.device`. For that signature only,
   ignore a referencing row only when its `sync_id` matches the sync performing
   the merge. That exemption holds because those rows live in main rather than
   in the branch: `delete_dcim_device` collects the branch-local cascade with
   `ForwardDeviceIdentity` in `ignored_related_models`, and the orphan is
   removed afterwards by `finalize_device_identities_locked` in post-merge
   bookkeeping. Nothing deletes them ahead of the device delete, and describing
   it that way would invite an entry whose relation has no such split. A
   peer-sync identity, tag claim, virtual-parent claim, or any other hidden
   reference remains a blocker.
4. Thread the ingestion's sync primary key from the production merge caller to
   the predictor. Direct utility callers that provide no sync context receive
   no exemption and therefore fail closed.

This intentionally does not infer behavior from app labels, inheritance,
`related_name`, or the presence of a `sync` field. Adding another exemption
requires naming the exact target, referencing model, and FK, then proving the
same-transaction cleanup path with tests.

## Validation

- `forward_netbox.tests.test_bulk_merge` fully green, including:
  - `test_owned_device_delete_preserves_provenance_until_atomic_merge`
  - `test_device_delete_serializes_concurrent_claim_writer`
  - `test_device_delete_serializes_concurrent_generic_relation_writer`
  - new hidden-PROTECT prediction/reporting regression
- `forward_netbox.tests.test_protecting_relations` fully green
- `invoke harness-check`
- Run tests in isolated compose project `forward-netbox-codex-pred` on host port
  `8139`, using a distinct `invoke test-isolated --project-name` value after
  migrations finish, and retain literal unittest summaries.

Evidence, 2026-08-02:

- Final clean isolated combined run:
  `Ran 99 tests in 511.834s` followed by `OK`.
- `invoke lint`: all hooks passed, including Black, Flake8, and sensitive
  content checks.
- `invoke harness-check`: passed.

## Rollback

Revert the predictor, caller, test, and this plan together. Hidden protection
would again reach apply as `ProtectedError`; no schema or persisted data cleanup
is required.

## Status

Shipped and wired. `protecting_relations()` enumerates hidden relations via
`_get_fields(..., include_hidden=True)` (`bulk_merge.py:1480`);
`protecting_reference_blocked_deletes(..., merge_sync_id=None)` is at `:1515`;
`_skip_protecting_reference_blocked_deletes` (`:1318`) is called from the main
merge at `:2187`, and `merge.py:651` supplies `merge_sync_id=ingestion.sync_id`
(`2484d33`, #122). The call site is pinned by an `inspect.getsource` assertion
in `test_protecting_reference_deletes.py` precisely because this predictor once
shipped with no caller at all.

The 2.9.2 release plan carried this as deferred after it had shipped. Verified
in the 2.9.2 tree before recording. Note the exemption is now largely moot: the
three plugin FKs became `SET_NULL` in migration 0051.

## Decision Log

- 2026-08-02: Rejected exempting every plugin provenance model. Claims and
  peer-sync identities are intentional blockers and must still be predicted.
- 2026-08-02: Chose an exact relation signature plus current-sync row scope.
  This mirrors the authoritative Device-delete guard's actual cleanup contract.
- 2026-08-02: Added no dependency edge. Ordering cannot make a surviving
  protected reference disappear, while a new edge would add cycle risk without
  resolving the row.
