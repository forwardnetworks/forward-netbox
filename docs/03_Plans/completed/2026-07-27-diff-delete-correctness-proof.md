# Diff Delete-Correctness Proof

## Goal

Make the diff enablement gate trustworthy: compare diff-path delete workloads
with forced-full targets for one pinned snapshot pair using the production
canonical identity contract, prove scope transitions and danger-set reducers,
and report actual per-map diff execution and benefit before enabling any model.

## Constraints

- Work only in `/tmp/forward-netbox-diffproof`; the concurrent
  `/tmp/forward-netbox-publish-261` tree is read-only.
- Use compose project `fnb-diffproof` and run long phases detached with durable
  progress/evidence checkpoints.
- Do not publish queries, mutate Forward/customer, bump versions, run release gates,
  commit, or make GitHub changes.
- Use `/home/captainpacket/customer.token` only through `ForwardClient._auth()` and
  never print credentials or customer identifiers.
- Keep live NQE executions minimal and record full versus diff call counts.
- Any spurious delete, comparator identity error, incomplete full oracle, or
  unexercised requested diff contract is a hard NO-GO for that model.

## Touched Surfaces

- `scratchpad/validation-2.6.2/detached_validation.py`
- `scratchpad/validation-2.6.2/full-cycle/full_cycle_validation.py`
- focused comparator tests under `scripts/tests/` or
  `forward_netbox/tests/`, selected after inspecting import boundaries
- this active plan and sanitized local proof artifacts

Production diff, reducer, and workload-state code will change only if the
corrected oracle demonstrates a real product defect.

## Approach

1. Replace the legacy `{model, fields, values}` digest in the oracle with
   `SHA256(canonical_row_identity(...))`, matching the already captured
   baseline and production special identities for cables and FHRP participants.
2. Make identity capture fail closed with model/query context. Test ordinary,
   alternate coalesce, cable, FHRP, incomplete identity, namespace mismatch,
   spurious, missing, staged-count mismatch, and upstream fetch/error cases.
3. Separate logical workload-delete proof from physical Branching
   `ChangeDiff` accounting. Map or explicitly classify derived/cascaded physical
   models instead of treating unlike model namespaces as equal.
4. Re-evaluate the previous nine physical count discrepancies using corrected
   identities and branch data to classify each as comparator/accounting artifact
   or product defect.
5. Run one detached customer-equivalent diff phase over the preserved pinned
   snapshot pair. Capture forced-full before/after canonical target identities,
   exact diff-path delete identities, staged physical deletes, model results,
   timings, and API usage.
6. Prove danger-set scope/reducer transitions with focused fixtures and the live
   oracle: an identity leaving customer scope must delete even when globally
   present; an identity entering scope must upsert and must not delete.
7. Assert per-map `nqe_diff_calls` for all 12 Tier 1 maps, seven Tier 2 model
   groups, MAC, and IPv4; report every full fallback and reason.
8. Compare populated-database full and diff staging/fetch evidence and report
   staged-row reduction, stage-time change, transfer work, and total NQE calls.

## Validation

- Focused comparator/oracle unit tests.
- Focused durable workload, query execution contract, contributor baseline,
  Tier 3 reducer, query fetch, and single-branch executor tests.
- `invoke harness-check`, `invoke harness-test`, `invoke lint`, and
  `git diff --check` in the isolated clone.
- Detached `fnb-diffproof` same-snapshot-pair oracle with checkpointed,
  sanitized evidence and explicit API/NQE usage.
- Per-model exact matched/spurious/missing report; zero spurious is mandatory.
- Per-contract diff/fallback coverage report and populated-database timing/row
  comparison.

## Rollback

Delete the isolated `/tmp/forward-netbox-diffproof` clone and its distinct
compose project/volumes. No Forward state, repository history, version, release,
or production state is changed. If production code is found defective, keep its
diff contract disabled until a separately reviewed fix passes this proof.

## Decision Log

- Rejected comparing the legacy digest with canonical baseline hashes: the two
  namespaces are disjoint and manufactured confident-looking spurious/missing
  totals.
- Rejected widening delete tolerance: any canonical spurious identity is a
  correctness failure.
- Rejected equating workload model counts directly with all physical
  `ChangeDiff` model counts: adapters and cascades can create related-model
  deletes, which require explicit accounting rather than silent mismatch.
- The corrected pinned-pair oracle matched all `1,434/1,434` canonical logical
  delete identities with `0` spurious and `0` missing. The prior raw
  `1,120,951` missing / `1,434` spurious result was entirely a mixed hash
  namespace artifact; the blank-VRF prefix error was also comparator-only.
- Physical staging is a second mandatory gate. The branch produced one
  `dcim.device` `ProtectedError` and two protected-dependency skips each for
  `dcim.interface` and `ipam.ipaddress`. The branch was not merged. Those
  models and the coupled BGP contract stay diff-disabled; exact workload
  intent is insufficient when apply convergence fails.
- The other raw physical-count differences were accounting effects: every
  cable delete creates two CableTermination deletes; FHRP group removal creates
  assignment/IP cascades; BGP peer removal cleans orphan router/scope rows; and
  already-absent inventory/tag targets are adapter no-ops.
- Coverage completed for `21/21` requested contracts: 19 native runtime diff
  calls plus two read-only calls for collapsed Tier 1 aliases. There were no
  failed calls or silent fallbacks in the requested set.
- Live Forward load was limited to 99 NQE executions: 58 in the actual hybrid
  run (39 query + 19 diff), 39 in the two forced-full oracle sides, and two
  supplemental diff calls.

## Completion Evidence

- Comparator and workload-state suite: `27/27` passed; workload normalization
  plus comparator suite: `43/43` passed.
- Repository harness: `invoke harness-check` passed; `invoke harness-test`
  passed (`208/208`).
- `invoke lint`, Python compilation, and `git diff --check` passed.
- Danger-set fixtures prove before-side ownership produces a delete for a row
  leaving scope, after-side ownership produces an upsert for a row entering
  scope, and Tier 3 contributor reduction occurs independently per side.
- Sanitized oracle evidence:
  `scratchpad/validation-2.6.2/diffproof-20260727/evidence/results/delete-correctness.json`.
- Sanitized coverage evidence:
  `scratchpad/validation-2.6.2/diffproof-20260727/evidence/results/diff-coverage.json`.
- No version, release, commit, GitHub, Forward publication, or production
  mutation was performed. The isolated defective branch remains unmerged.
