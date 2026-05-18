# Deferred Risk Tranche (Post-0.9.0)

## Goal

Execute the deferred long-term risk items in a controlled order without
re-opening solved 0.9.0 stability work:

1. compatibility branch-state cache retirement after compatibility window proof
2. live Forward runtime proof for deeper query pushdown
3. destructive Docker worker-kill recovery harness
4. faster apply engine path (`bulk_orm`) with strict parity gates

## Scope

This tranche is additive and risk-focused. It does not change user-facing
workflow semantics (`branching` and `fast_bootstrap` remain the two execution
backends).

## Constraints

- Keep NQE as the source-of-truth normalization layer.
- Keep native NetBox model semantics and Branching workflow behavior unchanged.
- Do not commit customer identifiers, snapshot IDs, network IDs, or raw customer
  row payloads in plan evidence or tests.
- Keep destructive failure injection outside default `invoke ci`.

## Touched Surfaces

- Plan/docs surfaces:
  - `docs/03_Plans/active/2026-05-16-deferred-risk-tranche.md`
  - `docs/03_Plans/completed/2026-05-15-scale-hardening-remaining-work.md`
- Expected code surfaces for follow-on implementation:
  - `forward_netbox/utilities/execution_ledger.py`
  - `forward_netbox/utilities/sync_state.py`
  - `forward_netbox/utilities/multi_branch_lifecycle.py`
  - `forward_netbox/utilities/resumable_branching.py`
  - `forward_netbox/utilities/query_fetch.py`
  - `forward_netbox/utilities/branch_budget.py`
  - `forward_netbox/utilities/apply_engine.py`
  - `tasks.py`
  - `forward_netbox/tests/*`

## Approach

Use a staged risk-first execution model:

1. Prove compatibility-window safety for compatibility branch-state cache while preserving upgrade
   behavior.
2. Gather ORG `0.9.1` real-run evidence before making deeper architectural moves.
3. Add live pushdown profiling evidence on selected maps.
4. Add opt-in destructive worker-kill harnessing.
5. Gate any `bulk_orm` implementation behind strict parity evidence.

## Sequencing

1. compatibility branch-state cache compatibility-window proof and removal plan
2. `0.9.1` ORG dataset stability validation (real-run evidence baseline)
3. live pushdown profiling and proof on selected high-volume maps
4. destructive worker-kill harness and recovery proof
5. `bulk_orm` parity spike for one simple model family (or explicit deferral)

## Workstream 1: compatibility branch-state cache Retirement Readiness

### Objective

Prove that compatibility branch-state cache is no longer a required control plane when an execution
run exists, then define the exact compatibility removal gate.

### Tasks

- Enumerate every compatibility write path (`setcompatibility branch-state cache_state`,
  `clearcompatibility branch-state cache_state`) and classify as:
  - required for old-state upgrade only
  - required for no-ledger fallback only
  - removable in next release window
- Add/extend tests that fail if active-run paths write compatibility state when
  the ledger is present.
- Publish explicit removal criteria for active writes in the next release window.

### Exit Criteria

- No active-run orchestration path depends on compatibility branch-state cache writes.
- Compatibility writes are isolated to upgrade/no-ledger fallback.
- Release note-ready removal checklist exists.

## Workstream 2: ORG `0.9.1` Real-Run Validation

### Objective

Collect real stability/performance evidence on the ORG dataset before deeper
architecture changes.

### Run Matrix

- Run A: full `branching` sync on `0.9.1` with query-id bindings.
- Run B: immediate second `branching` sync (steady-state behavior check).
- Run C: controlled interruption run (stop worker during staging, recover from UI).

### Evidence to Capture

- execution run/step summary
- shard count and per-phase timings
- created/updated/deleted counters
- row-failure counts and issue summaries
- support bundle exports (sync + execution run) for A/B/C

### Exit Criteria

- No unexpected fatal regressions versus late `0.9.0` behavior.
- Recovery controls remain actionable from native UI.
- Second run shows expected stabilization profile.

## Workstream 3: Live Query Pushdown Proof

### Objective

Validate whether a future parameterized `@query` pushdown shape can materially
reduce work and preserve output contract in live Forward runtime conditions.
The current stable path is native Forward column filtering plus NetBox-side
shard safety filtering; unvalidated shard parameters are intentionally not
shipped in built-in NQE.

### Tasks

- Select top slow maps from execution metrics.
- Compare:
  - full fetch
  - current column-filter pushdown
  - future parameterized `@query` guarded path
- Record runtime and fetched-row deltas.
- Confirm shape parity against full-query output for sampled shards.

### Exit Criteria

- Live evidence for at least one high-volume map family.
- Decision log entry: keep current pushdown only, expand pushdown, or defer until
  Forward query primitives improve.

## Workstream 4: Destructive Docker Worker-Kill Harness

### Objective

Add an opt-in local harness for real worker/process interruption scenarios that
synthetic tests cannot prove.

### Scenarios

- Kill stage worker before branch creation
- Kill stage worker after branch creation
- Kill stage worker during row apply
- Kill merge worker during merge queue/execution

### Implementation Notes

- Keep out of default `invoke ci`.
- Add dedicated command/task with explicit destructive warning.
- Require support-bundle export checks after each scenario.

### Exit Criteria

- Repeatable local command sequence exists.
- Recovery recommendations match actual post-kill state.
- Evidence can be attached to release readiness.

## Workstream 5: Faster Apply Engine (`bulk_orm`) Gate

### Objective

Either prove one safe `bulk_orm` candidate path or keep explicit deferral with
stronger parity evidence.

### Candidate Constraints

- Simple identity/coalesce behavior
- No side-effect object creation
- No Branching/change-log parity regressions
- Preserves row issue accounting and skip/failure semantics

### Exit Criteria

- `bulk_orm` remains opt-in.
- Each enabled model has focused parity tests before entering the safe set.
- Models with dependency creation, relationship side effects, or row-level
  special handling remain on the adapter path until their contract is proven.

## Validation Gates

Minimum for each code tranche:

```bash
invoke harness-check
invoke lint
invoke check
invoke scale-chaos-test
invoke test
invoke docs
```

Before release candidate:

```bash
invoke ci
```

For this deferred-risk tranche, ORG real-run evidence is required in addition to
local CI.

## Rollback

- Plan/docs rollback: remove this active plan and restore the completed-plan
  handoff pointer.
- Code rollback for follow-on tranches:
  - Keep feature work isolated by workstream so compatibility branch-state cache compatibility
    behavior, pushdown work, chaos harnessing, or apply-engine work can be
    reverted independently.
  - Keep destructive harness commands opt-in to avoid default CI impact.

## Decision Log

- Chosen: run deferred risks as a dedicated tranche so implementation and
  evidence stay ordered and auditable.
- Chosen: require ORG real-run validation before claiming stability improvements
  from deferred architecture work.
- Chosen: keep destructive worker-kill tests opt-in and outside default CI.
- Chosen: keep `bulk_orm` on an explicit deferred gate until native NetBox
  parity and row-level issue accounting can be proven beyond synthetic coverage.

## Completion Status

- compatibility branch-state cache control-plane retirement: complete.
  Active orchestration and run-state resolution now read ledger-only execution
  records; compatibility branch-run payload reads/writes are no longer used as a
  runtime control plane.
- live pushdown runtime proof: complete.
  Live `dcim.interface` evidence was captured via `forward_pushdown_profile`
  against `fwd.app`, including runtime/row-volume/parity diagnostics and query
  capability behavior.
- destructive worker-kill harnessing: complete.
  Opt-in chaos harness now supports scenario-aware readiness checks and optional
  execution-run support-bundle artifact capture.
- faster apply engine (`bulk_orm`): complete for the initial safe model set.
  `bulk_orm` is now enabled for a narrow scalar-core model set with tested
  fallback behavior for all other models.

## Validation Evidence

- `forward_netbox/utilities/sync_state.py` now suppresses compatibility
  compatibility branch-state cache writes when an active execution run exists (returns `False`
  instead of mutating sync parameters), preserving compatibility writes for
  no-ledger fallback only.
- Guard refined to preserve explicit compatibility linkage writes carrying
  `execution_run_id` during the compatibility window, after a focused regression
  in idempotent completion coverage.
- Added focused coverage in `forward_netbox/tests/test_sync_state.py`:
  - `test_setcompatibility branch-state cache_state_is_suppressed_when_execution_run_exists`
  - `test_setcompatibility branch-state cache_state_writes_when_no_execution_run_exists`
- Verification:
  - `ruff check forward_netbox/utilities/sync_state.py forward_netbox/tests/test_sync_state.py`
  - `manage.py test ...test_setcompatibility branch-state cache_state_is_suppressed_when_execution_run_exists ...test_setcompatibility branch-state cache_state_writes_when_no_execution_run_exists --keepdb --noinput`
  - `manage.py test forward_netbox.tests.test_sync_state forward_netbox.tests.test_ingestion_merge forward_netbox.tests.test_jobs.ForwardJobsTest.test_stage_forward_branch_item_uses_ledger_withoutcompatibility branch-state cache_json --keepdb --noinput`
- Added opt-in destructive harness entrypoint in `tasks.py`:
  - `invoke docker-chaos-kill --scenario=<stage-before-branch|stage-after-branch|stage-during-apply|merge-during-exec> --confirm=True`
  - Guarded so it refuses to run without explicit `--confirm=True`.
  - Intentionally excluded from `invoke ci`.
- Verification:
  - `ruff check tasks.py`
  - `invoke harness-check`
- Operator docs updated:
  - `docs/00_Project_Knowledge/validation-matrix.md` includes the destructive
    harness command matrix.
  - `docs/00_Project_Knowledge/release-playbook.md` includes the opt-in
    destructive harness as a release-adjacent gate for Branching recovery
    changes.
- Added live pushdown profiling command:
  - `forward_netbox/management/commands/forward_pushdown_profile.py`
  - `invoke pushdown-profile --sync-name <sync> --model <model> [--query-name ...]`
  - Reports full fetch vs shard-pushdown runtime, row counts, and parity deltas.
- Live pushdown profiling evidence captured:
  - `invoke pushdown-profile --sync-name "ui-harness-sync" --model "dcim.interface" --sample-shard-keys 10 --output-json /tmp/pushdown-dcim-interface-live.json`
  - Result: current repository query path required parameter fallback and
    returned full-row volume even with shard keys (`pushdown_fetch == full_fetch`)
    confirming deeper query-contract work remains required for true pushdown.
- Added scenario readiness probe command for destructive chaos runs:
  - `forward_netbox/management/commands/forward_chaos_probe.py`
  - `invoke docker-chaos-kill` now supports:
    - `FORWARD_CHAOS_SYNC_NAME` for scenario readiness polling
    - `FORWARD_CHAOS_OUTPUT_DIR` for execution-run support-bundle capture
  - Verified with:
    - `FORWARD_CHAOS_SYNC_NAME=ui-harness-sync FORWARD_CHAOS_OUTPUT_DIR=/tmp/chaos invoke docker-chaos-kill --scenario=stage-after-branch --confirm=True`
- Retired active compatibility branch-state cache runtime orchestration paths:
  - `forward_netbox/utilities/sync_state.py` now returns empty compatibility
    state for control-plane calls and uses execution ledger state exclusively.
  - `forward_netbox/utilities/execution_ledger.py::active_execution_run()`
    resolves active runs only from execution-run records.
- Enabled opt-in `bulk_orm` apply engine for the current safe model set:
  - `forward_netbox/utilities/apply_engine.py`
  - Models: `dcim.site`, `dcim.manufacturer`, `dcim.devicetype`, `ipam.vlan`,
    `ipam.vrf`
  - `dcim.devicerole` remains adapter-required with
    `blocker_code=tree_model_constraints` because nested-set fields are not
    preserved by `bulk_create`/`bulk_update` parity in this path.
  - Added coverage: `forward_netbox/tests/test_apply_engine.py`
