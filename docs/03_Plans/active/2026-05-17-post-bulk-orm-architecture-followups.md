# Post Bulk ORM Architecture Follow-ups (2026-05-17)

## Goal

Execute Phase I boundary extraction for `execution_ledger.py` and `health.py` without behavior changes, then continue through the remaining architecture follow-ups in order.

## Constraints

- No behavior regressions.
- Preserve API/UI contracts and summary keys.
- Extraction/delegation only for this tranche.

## Touched Surfaces

- `forward_netbox/utilities/execution_ledger.py`
- `forward_netbox/utilities/execution_ledger_*.py`
- `forward_netbox/utilities/health.py`
- `forward_netbox/utilities/health_*.py`
- `docs/03_Plans/active/2026-05-17-post-bulk-orm-architecture-followups.md`

## Approach

1. Extract cohesive clusters into focused utility modules.
2. Keep existing public entrypoints in original files as wrappers.
3. Validate after each extraction with harness/test checks.
4. Update this plan with completed and newly discovered items.

## Rollback

- Revert wrapper delegation for the affected module if a regression appears.
- Keep boundary extraction changes isolated so rollback can be scoped per module.

## Decision Log

- 2026-05-17: Execute Phase I first (`execution_ledger.py`, `health.py`) before moving to remaining Priority 1 items.

## Objective

Capture the next architectural refactor tranche after expanding eligible bulk ORM coverage, with a focus on maintainability, testability, and lower risk of regressions.

## Current Baseline

- Bulk ORM eligibility now includes:
  - `dcim.site`
  - `dcim.manufacturer`
  - `dcim.devicerole`
  - `dcim.platform`
  - `dcim.devicetype`
  - `ipam.vlan`
  - `ipam.vrf`
- Full validation gate is passing (`invoke ci`).

## Prioritized Follow-ups

## Priority 1 (high value, low behavior risk)

1. Split `forward_netbox/utilities/execution_ledger.py` (`~1491` lines)
   - Target boundaries:
     - `run_store` (run/step lifecycle persistence)
     - `metrics_store` (timing/counter persistence)
     - `reconciliation_store` (reconciliation event persistence)
     - `serialization` (payload shaping for API/UI/reporting)
   - Constraint:
     - No behavior changes; extraction only.

2. Split `forward_netbox/utilities/health.py` (`~1021` lines)
   - Target boundaries:
     - apply-engine health summary
     - fetch-contract health summary
     - diff-readiness summary
     - recommendation/support-bundle summary
   - Constraint:
     - Preserve all existing summary keys and semantics.

3. Split `forward_netbox/utilities/apply_engine.py` (`~882` lines)
   - Target boundaries:
     - decision/classification
     - simple bulk ORM path
     - tree-safe bulk ORM path
     - shared lookup/normalization helpers
   - Constraint:
     - Keep external decision codes and audit matrix behavior stable.

4. Split query resolution vs. query execution
   - Files:
     - `forward_netbox/utilities/query_binding.py` (`~879` lines)
     - `forward_netbox/utilities/query_fetch.py` (`~877` lines)
   - Target boundaries:
     - binding resolution (`query`, `query_id`, `query_path`, commit pinning)
     - runtime fetch execution (pagination, pushdown, concurrency)
   - Constraint:
     - Preserve current diff/query-id/query-path behavior and diagnostics.

## Priority 2 (after Priority 1)

5. Reduce `sync_runner_adapters.py` and `sync_routing.py` fan-in
   - Keep orchestration dispatch thin.
   - Move per-domain adapter registries into explicit modules (`core`, `ipam`, `routing`, `cable`, `inventory`).

6. Decompose `forward_api.py`
   - Separate auth/session, proxy/TLS config, retry policy, and pagination helpers.
   - Goal: easier unit testing and safer API surface changes.

## Out of Scope for This Tranche

- Branching workflow redesign.
- New apply engines (TurboBulk/parquet) on main.
- Data model/schema migrations unrelated to boundary extraction.

## Execution Order

1. `execution_ledger.py`
2. `health.py`
3. `apply_engine.py`
4. `query_binding.py` + `query_fetch.py`
5. `sync_runner_adapters.py` + `sync_routing.py`
6. `forward_api.py`

## Phase I Scope (Current Execution)

Phase I is limited to Priority 1 items 1-2:

1. `execution_ledger.py` boundary extraction
2. `health.py` boundary extraction

## Phase I Progress

- [x] `execution_ledger.py` initial boundary extraction
  - Added `forward_netbox/utilities/execution_ledger_metrics.py`
  - Added `forward_netbox/utilities/execution_ledger_serialization.py`
  - Wired `execution_ledger.py` to delegate metrics and support-bundle shaping.
  - Behavior target: unchanged contract (delegation only).
- [x] `execution_ledger.py` remaining extraction pass
  - [x] Move reconciliation/retry/discard cluster into dedicated module:
    - `forward_netbox/utilities/execution_ledger_reconciliation.py`
  - [x] Move run-store/lifecycle mutation cluster into dedicated module:
    - `forward_netbox/utilities/execution_ledger_run_store.py`
- [x] `health.py` extraction pass
  - Split summary assembly into dedicated domain modules while preserving key contract.
  - [x] Extract model/apply-engine/fetch-contract summaries:
    - `forward_netbox/utilities/health_apply_fetch.py`
  - [x] Extract source/runtime/query-map/validation/ingestion/execution-run/capacity summaries:
    - `forward_netbox/utilities/health_summary_blocks.py`
  - [x] Extract health checks/recommendation assembly domains:
    - `forward_netbox/utilities/health_checks.py`

## New Items Discovered During Phase I

1. `execution_ledger.py` has two dense clusters still in-file after initial extraction:
   - run/step lifecycle mutation methods
   - reconciliation/retry/discard flow
   - Update: reconciliation/retry/discard flow has now been extracted.
2. Existing constants and status-mapping helpers are shared across these clusters;
   extracting them should be done with shared contract constants to avoid drift.
3. `health.py` extraction now delegates all major summary/check clusters through focused modules:
   - `health_apply_fetch.py`
   - `health_summary_blocks.py`
   - `health_checks.py`
   - remaining in-file functions are compact wiring/probe helpers and wrappers.

## Phase I Exit Evidence

- `invoke harness-check` passed.
- `invoke harness-test` passed.
- `invoke check` passed.
- `invoke test` passed.
- `invoke ci` passed.

## Remaining Items (Post-Phase-I)

Phase I scope is complete. Remaining work moved to the canonical Phase II plan:

- `docs/03_Plans/active/2026-05-17-phase-ii-architecture-followups.md`

Canonical remaining items are:

1. `apply_engine.py` boundary extraction
2. Query resolution vs query execution split
3. `sync_runner_adapters.py` + `sync_routing.py` fan-in reduction
4. `forward_api.py` decomposition

## Validation Gate (per step)

- `invoke harness-check`
- `invoke harness-test`
- `invoke test`
- `invoke ci` (at phase boundaries)

## Completion Criteria

- No behavior regressions.
- No API/UI contract drift.
- Architecture audit remains green:
  - no classification gaps
  - no unclassified fallback decisions
  - no fetch-contract coverage gaps
