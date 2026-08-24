# Measure drift for cables

## Goal

Slice two of the adapter-only drift comparison begun in
`2026-08-24-adapter-model-drift-taggeditem.md`: `dcim.cable`.

## Why

Same as slice one. Every adapter-only model reports a workload upper bound, so
`in_sync` is unanswerable while any of the eight remains uncompared. Cables are
the second smallest and the natural next step: single phase, no recursive
`_ensure_*` chain, and thirteen existing regression tests around the apply.

## Constraints

- Reuse the apply's own resolution; do not reimplement the comparison.
- The preview must write nothing - and for cables that means NetBox core's
  writes too, not just this module's.
- A row the apply refuses must not be counted as drift.

## Touched Surfaces

- `forward_netbox/utilities/sync_cable.py` - `apply_dcim_cable` gains
  `preview`
- `forward_netbox/utilities/drift_comparison.py` - the shared adapter loop,
  the cable comparison, three missing runner shims, and one shim defect fixed
- `forward_netbox/tests/test_cable_drift_comparison.py` (new)

## Approach

### A latent defect in the preview runner, found before it could bite

`PreviewRunner` seeded `self._conflict_policy = {}` - a dict. On the real
runner `_conflict_policy` is a METHOD (`sync_runner_contracts.py:50`), and
every caller invokes it. Nothing reads the dict form, so the mistake was
invisible: the bulk paths write through `bulk_create` and never ask for a
policy.

Every adapter model writes through `coalesce_upsert`, which asks on every row.
The first adapter model to reach it would have died on
`TypeError: 'dict' object is not callable`. It is now a method reading the
same `MODEL_CONFLICT_POLICIES` table the apply reads - not a local default,
because the policy decides whether a conflicting row is skipped or raises, and
a preview that disagreed about that would classify a row the apply refuses as
one it would write.

This is the same class of finding as `_ensure_vrf` in the bulk work: a
mismatch between preview and apply that no grep would surface, found only by
reading what the path actually calls.

### The writes are direct, so the firewall does not help

Tagged items had one write behind a `runner.` call and one M2M write outside
it. Cables have neither shape: `cable.save()` for a status change and
`Cable(...).save()` for a new cable are both direct, and `Cable.save()`
additionally makes NetBox core persist the two `CableTermination` rows that
are the durable relationship (the `Interface.cable` cache is not, per this
module's own note about Branching).

So the preview returns at three points, each before its write. The create
return is placed before `Cable(...)` is even constructed, and the test asserts
on `CableTermination` count rather than only `Cable` count - so a return
placed one line later would fail.

### The refusals are the interesting half

Two rows the apply declines:

- a LAG endpoint, because NetBox does not allow a cable terminated directly to
  a LAG;
- an interface already cabled to something else, which under the default
  `strict` policy raises and under `skip_warn_aggregate` warns and skips.

Counting either as a create would report drift that no apply could ever
resolve - the report would show a permanent non-zero that re-running never
clears, which is the "re-running cannot change this" complaint that opened
#206 in the first place, wearing different clothes. Both count as `rejected`.

### The loop is now shared

`_compare_adapter_rows` takes the apply function and does the iteration,
exception handling and tallying. Both adapter models use it, and the remaining
six will. It treats a `False`/`None` return - the apply functions' own "I
declined this row" answer - as rejected, and refuses the whole model on an
outcome it does not recognise rather than defaulting it to `unchanged`.

## Validation

13 tests. Both writes were proven guarded by their own negative control, run
separately:

- Removing the create guard failed
  `test_a_preview_creates_no_cable_and_no_terminations` and
  `test_an_uncabled_pair_is_a_create`.
- Removing the update guard failed
  `test_a_preview_does_not_rewrite_a_drifted_status` and
  `test_a_cable_whose_status_drifted_is_an_update`.

Two parity tests apply for real afterwards and assert the preview predicted
the write count.

Full run: 313 tests green across both adapter suites, the bulk drift suite,
the preview contract and priming suites, and `ForwardSyncRunnerTest`.

## Rollback

Remove `dcim.cable` from `_ADAPTER_COMPARISONS`; it returns to an upper bound.
`preview` defaults to `False`, so every existing caller is unchanged. The
`_conflict_policy` fix is not part of that rollback and should stay - it
corrects the shim regardless of which models are wired up.

## Decision Log

- **Read `MODEL_CONFLICT_POLICIES` rather than defaulting to `strict` in the
  shim.** A local default would have been simpler and would have made the
  preview disagree with the apply for exactly the models that configure a
  policy.
- **Assert on `CableTermination`, not just `Cable`.** The termination rows are
  what NetBox core writes inside `save()`, and they are the assertion that
  catches a guard placed too late.
- **Refusals count as `rejected`, not `creates`.** A permanent non-zero drift
  figure that re-running cannot clear is the original #206 complaint.

## Open

- Six models remain: inventory items, FHRP groups, lifecycle (netbox-dlm),
  modules, peering, routing.
- Modules will need the same care as cables and more: `Module.save()` with
  `_adopt_components` makes NetBox core instantiate component rows, and the
  path writes Manufacturer, ModuleType and ModuleBay beneath it.
- Routing and peering last, and together: peering is 30 lines but calls
  `_ensure_netbox_routing_bgppeer` first, inheriting a 7-deep write chain.
