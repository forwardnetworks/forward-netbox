# Phase II Architecture Follow-ups (2026-05-17)

## Goal

Execute all remaining architecture follow-up items after Phase I completion, preserving behavior while reducing module fan-in and improving maintainability/testability.

## Constraints

- No behavior regressions.
- Preserve API/UI contracts and existing diagnostics.
- Extraction/delegation first; avoid semantic rewrites unless required for parity.

## Touched Surfaces

- `forward_netbox/utilities/apply_engine.py`
- `forward_netbox/utilities/query_binding.py`
- `forward_netbox/utilities/query_fetch.py`
- `forward_netbox/utilities/sync_runner_adapters.py`
- `forward_netbox/utilities/sync_routing.py`
- `forward_netbox/utilities/forward_api.py`
- `docs/03_Plans/active/2026-05-17-post-bulk-orm-architecture-followups.md`
- `docs/03_Plans/active/2026-05-17-phase-ii-architecture-followups.md`

## Approach

1. Execute each remaining item as a boundary extraction with compatibility wrappers.
2. Validate after each item (`harness-check`, `harness-test`, `check`, `test`).
3. Run full `invoke ci` at tranche boundaries.
4. Keep this file as the canonical progress ledger for remaining architecture work.

## Rollback

- Revert only the affected extraction module(s) and wrapper wiring for a failed item.
- Keep changes item-scoped so rollback does not require reverting unrelated completed items.

## Decision Log

- 2026-05-17: Phase II starts after Phase I closure; include all remaining items in one active plan.

## Scope

All remaining items from `2026-05-17-post-bulk-orm-architecture-followups.md`:

1. `apply_engine.py` boundary extraction
2. Query resolution vs execution split (`query_binding.py`, `query_fetch.py`)
3. `sync_runner_adapters.py` + `sync_routing.py` fan-in reduction
4. `forward_api.py` decomposition

## Progress

- [x] 1) `apply_engine.py` boundary extraction
  - [x] Extract decision/classification cluster:
    - `forward_netbox/utilities/apply_engine_decision.py`
  - [x] Extract bulk apply + helper cluster:
    - `forward_netbox/utilities/apply_engine_bulk.py`
  - [x] Rewire `apply_engine.py` as compatibility wrapper surface
  - [x] Validate and close item 1
- [x] 2) Query resolution vs execution split
  - [x] Extract query execution surface:
    - `forward_netbox/utilities/query_fetch_execution.py`
  - [x] Rewire `query_fetch.py` as compatibility wrapper with patch-bridge behavior
  - [x] Extract query binding surface:
    - `forward_netbox/utilities/query_binding_resolution.py`
  - [x] Rewire `query_binding.py` as compatibility wrapper
- [x] 3) Adapter/routing fan-in reduction
  - [x] Extract routing implementation surface:
    - `forward_netbox/utilities/sync_routing_impl.py`
  - [x] Rewire `sync_routing.py` as compatibility wrapper surface
- [x] 4) `forward_api.py` decomposition
  - [x] Extract Forward API implementation surface:
    - `forward_netbox/utilities/forward_api_impl.py`
  - [x] Rewire `forward_api.py` as compatibility wrapper surface with patch-bridge behavior

## Remaining Items (Current)

Phase II is complete; no remaining items.

## Validation Evidence

- 2026-05-17: Item 1 validation gate passed after apply-engine boundary extraction.
  - `invoke harness-check`: pass
  - `invoke harness-test`: pass
  - `invoke check`: pass
  - `invoke test`: pass
- 2026-05-17: Item 2 validation gate passed after query binding/fetch extraction.
  - `invoke harness-test`: pass
  - `invoke check`: pass
  - `invoke test`: pass
- 2026-05-17: Item 3 validation gate passed after routing extraction.
  - `invoke harness-test`: pass
  - `invoke check`: pass
  - `invoke test`: pass
- 2026-05-17: Item 4 validation gate passed after forward API extraction.
  - `invoke harness-test`: pass
  - `invoke check`: pass
  - `invoke test`: pass

## Validation Gate

- `invoke harness-check`
- `invoke harness-test`
- `invoke check`
- `invoke test`
- `invoke ci` (phase boundary)

## Completion Criteria

- All items 1-4 complete with no behavior regressions.
- Full `invoke ci` passes after final item.
