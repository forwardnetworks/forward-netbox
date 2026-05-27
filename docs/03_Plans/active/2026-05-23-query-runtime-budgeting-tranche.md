# 2026-05-23 Query Runtime Budgeting Tranche

## Goal

Implement runtime-aware branch plan shaping so shard sizing reflects both
NetBox branch safety limits and real query/runtime behavior, while preserving
existing contracts and Branching-native execution.

## Constraints

- NQE remains the source of truth for row shaping and normalization.
- NetBox native model mutations remain unchanged.
- Branching safety cap (`max_changes_per_branch`) remains a hard upper bound.
- Runtime shaping must be bounded and deterministic.

## Touched Surfaces

- `forward_netbox/utilities/branch_budget.py`
- `forward_netbox/tests/test_sync.py`
- `docs/03_Plans/active/2026-05-23-long-term-architecture-roadmap.md`
- `docs/03_Plans/active/2026-05-23-query-runtime-budgeting-tranche.md`

## Current State

- Long-term roadmap is active in
  `docs/03_Plans/active/2026-05-23-long-term-architecture-roadmap.md`.
- Query fetch already records per-model runtime (`query_runtime_ms`) during
  workload build.
- Branch planning already supports row budgets, density-aware shaping, and
  delete-heavy safeguards.
- Pushdown efficiency observability is in place (health + support bundle).

## Problem Statement

Current planning is primarily row/density-driven. It does not yet use runtime
pressure directly to shape shard fanout. This can produce avoidable queue depth
and long tail latency for hot models.

## Scope

### In Scope

1. Add runtime-aware budget shaping in planner logic.
2. Keep hard branch safety bounds (`max_changes_per_branch`) unchanged.
3. Keep existing adapter behavior and row contracts unchanged.
4. Add targeted tests for runtime-based decisions and invariants.
5. Record status/evidence back into the active roadmap.

### Out of Scope

- New model contracts.
- NQE query semantic changes.
- Release/version branching strategy changes.

## Approach

1. Add a bounded runtime-shaping factor on top of existing density/delete-aware
   row budgets.
2. Apply shaping only for sufficiently large workloads and valid runtime data.
3. Prevent runtime-based widening for delete-heavy shards to preserve existing
   conservative delete behavior.
4. Validate with focused planner tests plus required repo gates.

## Design

Use `query_runtime_ms` plus workload size/density to derive a bounded runtime
pressure factor per model and adjust effective planning budget:

- High runtime pressure:
  - reduce fanout inflation and avoid over-sharding amplification.
- Low runtime pressure + low density:
  - allow wider shards (still under branch safety cap).
- Merge/apply-heavy pressure:
  - prefer narrower shards where overflow risk is higher.

All adjustments remain bounded and reversible, and never exceed configured
branch limits.

## Implementation Surfaces

- `forward_netbox/utilities/branch_budget.py`
  - add runtime-pressure helpers and bounded budget shaping.
- planning call site(s) in execution path
  - pass runtime-aware inputs through existing planner interfaces.
- tests:
  - planner runtime shaping behavior and safety invariants.

## Validation Plan

Minimum validation before merge:

1. Targeted tests for planner runtime shaping.
2. `poetry run invoke harness-check`
3. `poetry run invoke check`
4. `poetry run invoke ci` (or equivalent CI confirmation before release)

## Done Criteria

- Planner uses runtime signals deterministically.
- No violation of branch budget hard limits.
- Existing plan/adapter behavior remains contract-compatible.
- Roadmap ledger updated with status + validation evidence.

## Rollback

- Revert runtime-shaping helper and constants in `branch_budget.py`.
- Re-run existing density/delete-only budget behavior.
- Keep tests and roadmap notes aligned to the reverted behavior.

## Decision Log

- Runtime shaping is layered onto the existing budget path to avoid introducing
  a parallel planner.
- Delete-heavy widening is explicitly blocked to avoid reintroducing large
  delete-shard instability.
