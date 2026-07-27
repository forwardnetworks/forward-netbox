## Goal

Admit the customer's complete 28-model / 32-map first baseline to the disabled-by-default direct-load path only when every derived delete, relationship row, dependency, and owned side effect is proven equivalent to the ordinary branch path, then measure the real clean baseline and populated steady-state run.

## Constraints

- Preserve the existing dirty worktree and branch `fix/2.6.2-runtime-regressions`.
- No version bump, release gate, commit, GitHub change, Forward write, production NetBox mutation, or customer identifier in durable evidence.
- Keep normal steady-state execution on the single Branching branch path.
- Keep fast-baseline selection fail closed before target DML and atomic after target DML.
- Preflight creates no ingestion, branch, ObjectChange, workload state, contributor state, identity, or target row.
- Use one distinct disposable Compose project; run long validation detached with phase checkpoints.

## Touched Surfaces

- `forward_netbox/utilities/fast_baseline.py`
- `forward_netbox/utilities/fast_baseline_models.py`
- `forward_netbox/utilities/branch_budget.py`
- `forward_netbox/utilities/branch_lifecycle.py`
- `forward_netbox/utilities/workload_normalization.py`
- `forward_netbox/utilities/bulk_merge.py`
- `forward_netbox/tests/test_fast_baseline.py`
- `forward_netbox/tests/test_workload_normalization.py`
- `forward_netbox/tests/test_bulk_merge.py`
- `scripts/benchmark_fast_baseline.py` or a focused full-configuration validation harness
- fast-baseline architecture, operator, and reference documentation

## Approach

1. Give `BranchWorkload` and plan items an explicit, immutable derived-delete contract. Mark only the full `netbox_dlm.cve` rows excluded by authoritative full vulnerability coverage as `cve_without_in_scope_vulnerability_v1`, and only when the incoming workload had no other delete rows. Preserve the excluded rows in durable workload state exactly as today.
2. Admit only that contract on the fast path. Under the existing locks and empty-target proof, omit its physical delete calls and attest the omitted no-op count. Reject source deletes, native diffs, bootstrap deletes, mixed/unmarked deletes, or any future derived-delete class.
3. Add versioned direct-to-main row contracts for the ten requested models. Use the normal adapter for relationship models whose required behavior includes model-owned side effects; the fast path's specification is the pinned row/dependency contract plus that exact adapter, not a new normalization implementation.
4. Prove complete identities and dependency coverage across the fetched workload before DML: devices/sites/interfaces/VRFs, module bay/type inputs, tag identity consistency, FHRP participant/VIP conflicts, BGP peer/scope/address-family dependencies, and OSPF area/instance/interface dependencies. Reject ambiguity, duplicate conflicting identities, invalid enums/addresses/ASNs/process IDs, or missing parents.
5. Resolve `dcim.module` versus inventory admission by excluding only module-native inventory rows from physical InventoryItem creation when Module sync is enabled, matching the existing adapter's empty-target behavior, while retaining their workload-state rows and recording them as intentional skips. Non-module inventory rows remain on the set-based loader.
6. Lock and prove empty every side table the admitted adapters can create or relate: tags/assignments, module bays/types, FHRP assignments/VIPs, routing ASNs/router/scope/address families, and relevant generic-relation tables. Include their canonical state in paired equivalence.
7. Avoid a second Forward fetch in any combined preflight/load run by carrying the already validated in-memory fetcher/context/workloads into execution. Keep the standalone complete preflight honest: its slowest full NQE remains unavoidable because full/delete/cross-model proofs cannot be established from a sample. Report fetch and local-proof time separately.

## Validation

- Unit tests for marked versus unmarked deletes, state tombstone preservation, no physical delete invocation, locked empty-state recheck, and attestation counts.
- Per-model admission and rejection tests for every requested model, including invalid/missing/duplicate relationship dependencies and module-native inventory handling.
- Paired ordinary/fast fixtures comparing target counts, canonical fingerprints, owned and model-created side tables, logical totals, lifecycle, and issues.
- `invoke harness-check`, focused fast-baseline/workload-normalization/sync tests, formatting, and `git diff --check`; no release gate.
- Disposable full 28-model / 32-map preflight with protected-state zero-mutation proof and Forward API counters.
- Disposable full baseline with transaction wall, rows, statements/change, peak RSS, issues, attestation, and rate samples.
- Same populated database normal branch sync with staged change count/time and merge time/rate.
- Ordinary-path reference comparison plus orphan, skip, spurious-delete, and relationship-integrity queries.

## Rollback

Remove the new model specs and derived-delete contract from the allowlist. Existing syncs then fail closed to the ordinary branch path before DML. Disposable Compose volumes and redacted evidence can be removed independently; no production data migration is introduced.

## Decision Log

## Validation Outcome

- Full 28-model / 32-map clean preflight: eligible; 179.500-second fetch plus
  3.433-second local proof; protected state unchanged.
- Full direct baseline: completed without issues; 1,168,250 logical creates;
  3,289.205-second target transaction; all 28 specs durably attested.
- Same-snapshot canonical comparison: all 28 targets and explicit dependency
  side tables equal; only the expected ownership-reconciliation lifecycle row
  differed.
- Populated next-snapshot ordinary path: staged 19,180 changes in 614.301
  seconds; merge applied 19,179 at 60.560/s before one protected DLM software
  version delete failed. Branch remained ready; partial state has zero FK/GFK
  orphan groups.
- Decision: NO-GO until the protected SoftwareVersion delete is suppressed or
  ordered safely and a complete ordinary merge plus final equivalence proof
  passes. Keep this plan active.

## Decision Log

- Reject a staged partition: it was already measured and does not solve the CVE admission failure.
- Reject a physical CVE delete pass on an empty baseline: it adds statements but cannot change target state. Preserve the logical tombstone instead.
- Reject blanket delete admission: later plan items and side-effect models can make an initially empty related table nonempty, so only an explicit normalization class with an empty-target proof is safe.
- Reject bounded row sampling as a replacement for the complete standalone preflight: samples cannot prove absence of late shape violations, duplicate identities, or cross-model dependency conflicts.
- Prefer the existing relationship adapters over plausible set-based loaders because those adapters own generic relations, dedupe/skip semantics, module component adoption, and routing/FHRP side objects.
- Treat a full Device workload as authoritative for full InventoryItem parent
  coverage. Rows for absent devices are unrepresentable in either engine and
  are normalized out with an aggregate reason; partial/diff device workloads
  do not trigger that filter.
- Treat CableTermination as the authoritative existing-link relation on later
  normal syncs. Branch merge does not retain Interface's optional cable cache;
  consulting only that cache caused every unchanged populated-baseline cable
  to be retried as a conflicting create. The normal adapter now falls back to
  the termination relation while preserving the paired final-state contract.
- The failed SoftwareVersion delete was legitimate, not a permanent customer
  protection case. Two main-schema DeviceSoftware rows still referenced the
  retiring version. Their branch updates reassigned both rows, but the bulk
  branch ObjectChanges had no old-FK preimage, so Branching's dependency graph
  ordered the protected delete first; both releases applied 207.9 seconds after
  the failed delete. Restore the missing dependency from batched authoritative
  destination FK values. Retained references still fail strictly.

## 2026-07-27 End-to-End Outcome

- Added destination-FK release dependencies to bulk-merge ordering and an exact
  DLM regression. The focused integration test applied the two releases before
  the protected SoftwareVersion delete (`2 applied, no failed`). No exception is
  suppressed; retained references still stop the merge.
- Rerun ordinary same-snapshot state is identical to the fast baseline for all
  28 target counts/fingerprints, all eight explicit side-table comparisons, and
  zero issues/orphans. The ordinary run is not an exact no-op: 360 BGP peer
  representational updates remain.
- Fresh preflight and direct baseline passed. The baseline completed 1,158,864
  logical changes in 3,490.460 seconds with zero issues. IPAddress degradation
  persisted (65.329 to 145.181 seconds across equal shards).
- The next-snapshot ordinary sync completed: 16,852 applied, zero merge
  failures, 366.886-second stage, 265.288-second merge. Its live snapshot did
  not contain the SoftwareVersion deletion reproduced by the regression.
- The third same-snapshot sync failed the true-steady-state criterion: ten
  deferred cable replacements produced 50 merge changes.
- Native diff execution was observed (19 diff calls), and the timeout and
  incompatible-contract guards passed. The diff/delete oracle failed closed:
  the validation identity domains were inconsistent and nine staged delete
  counts differed from forced-full results. No diff branch was merged.
- Final decision remains **NO-GO**. Keep this plan active. No version bump,
  release gate, commit, GitHub action, or production mutation was performed.
