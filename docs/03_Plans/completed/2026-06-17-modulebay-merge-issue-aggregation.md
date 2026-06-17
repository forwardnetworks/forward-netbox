# Module-Bay Merge Issue Aggregation

## Goal

Stop a single new device with module-bay templates from flooding the ingestion
issues list with dozens of identical, cryptic `dcim.modulebay` merge failures.
Replace them with one actionable issue that tells the operator exactly how to
remediate (import module bays via `forward_module_readiness`).

## Constraints

- NetBox Branching cannot create MPTT module bays during a merge: `ModuleBay`
  has a custom `save()` that takes an UPDATE path when Branching deserializes the
  create with a pk, so the row never lands in `main` and the change fails with
  `NotUpdated` ("Save with update_fields did not affect any rows"). This is a
  NetBox limitation; the plugin cannot fix it in the merge path.
- Must not hide real plugin sync failures — only the replication side-effect
  model (`dcim.modulebay`) is aggregated; models the plugin syncs directly keep
  one issue per change.
- Must not change which changes apply: device and interface sync are unaffected;
  module bays remain imported out-of-band via `forward_module_readiness`, per the
  v1.4.1.1 module-bay merge hardening.
- Change-count accounting (`applied`/`failed`) must stay accurate.

## Touched Surfaces

- `forward_netbox/utilities/merge.py` — new `_MergeIssueRecorder`,
  `REPLICATION_SIDE_EFFECT_MODELS`, `MODULE_BAY_MERGE_REMEDIATION`.
- `forward_netbox/tests/test_ingestion_merge.py` — `MergeIssueRecorderTest`.
- `docs/01_User_Guide/usage.md` — note on the MPTT/branching limitation and the
  readiness remediation.

## Approach

Extract the per-change failure handling from `merge_branch` into a
`_MergeIssueRecorder`. For each failed change it either records one
`ForwardIngestionIssue` (models the plugin syncs) or, for
`REPLICATION_SIDE_EFFECT_MODELS` (`dcim.modulebay`), accumulates a count plus a
sample error. On `flush()` (after the merge loop) it emits a single
`ModuleBayMergeUnsupported` issue per side-effect model with the
`MODULE_BAY_MERGE_REMEDIATION` text pointing at `forward_module_readiness`.
`failed` is still incremented per change so totals are exact.

## Validation

- `invoke test --test-label forward_netbox.tests.test_ingestion_merge` (new
  `MergeIssueRecorderTest`: aggregate vs per-change).
- `invoke test --test-label forward_netbox.tests.test_sync` (regression).
- Reproduced live: a real netbox_branching branch with a device (root bay) and a
  module (nested bay), merged via `merge_branch`; confirmed the device merges,
  both bays fail, and the failures collapse to one issue.
- `invoke lint`, `invoke harness-check`.

## Rollback

Revert the `merge.py`, test, and docs changes. No schema or data migration; the
change only affects how merge-time failures are recorded, not applied data.

## Decision Log

- Rejected disabling NetBox component replication on device/module create
  (`_disable_replication`): it is all-or-nothing and would drop module interface
  creation, since the interface map does not set module association.
- Rejected adding a `dcim.modulebay` sync map: branch-created module bays hit the
  same MPTT merge limitation (the reason v1.4.1.1 moved to out-of-band import).
- Rejected silently suppressing the failures: module bays genuinely do not merge,
  so the operator must be told to run `forward_module_readiness`.
