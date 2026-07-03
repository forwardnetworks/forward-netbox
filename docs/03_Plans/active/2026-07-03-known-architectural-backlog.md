# Known architectural backlog (tracked, not scheduled)

**Date:** 2026-07-03
**Source:** post-2.2.5 read-only architecture audit (4 agents).

## Goal
Keep a durable register of the genuine structural items the audit surfaced, with
an explicit defer/blocked rationale for each, so they are tracked rather than lost.
This document schedules no work; it is the backlog the next planning pass draws
from.

## Constraints
- These items are project-sized, migration- or correctness-sensitive, or blocked
  on field data — none is a safe drive-by edit.
- The audit confirmed the source is otherwise clean (no dead code, no TODO/FIXME,
  no version shims), so this register is deliberately short.

## Touched Surfaces
None — this is a tracking document. Each item below names the code it would touch
if/when scheduled.

## Approach
Record each item with its current code status and a one-line verdict. Revisit at
the next planning pass; promote an item to its own plan only when it is scheduled.

## Backlog items

### 1. Dual apply engine + per-model parity matrix — DEFER (deliberate migration)
`apply_engine_decision.py` gates every model between the adapter path
(`sync_runner_adapters.py`) and the bulk-ORM path (`apply_engine_bulk.py`) via
hand-maintained sets (`BULK_ORM_ENABLED_MODELS`, `ADAPTER_REQUIRED_MODELS`,
`SIMPLE_BULK_CANDIDATE_MODELS`, `BULK_ORM_PARITY_GATES`). Both paths are
load-bearing; each model is promoted individually behind a parity test. Largest
structural tax, but an intentional in-flight migration. Converging to bulk-only is
a multi-release project, not a cleanup edit.

### 2. Unreachable `multi_branch` scaffolding — DEFER (needs careful removal)
Single-branch is the only executor (`sync_orchestration.py` only builds
`ForwardSingleBranchExecutor`), yet a `multi_branch` concept still threads through
`models.py`, `sync_state.py`, `model_validation.py`,
`sync_facade.uses_multi_branch`, and `multi_branch_lifecycle.py`. Removal touches
persisted sync `parameters` and the param allowlist, so it wants its own change +
migration + test pass.

### 3. 10k-changes/branch budget + density learning — DEFER (audit for over-engineering)
`branch_budget.py` (`DEFAULT_MAX_CHANGES_PER_BRANCH = 10000`) plus
`density_learning.py` (EWMA density estimator) drive sub-batching. With
single-branch now the norm, whether the full density apparatus still earns its
complexity is worth a dedicated audit — it may reduce to a fixed sub-batch size.

### 4. 1-created/1-deleted idempotency churn — BLOCKED (needs field data)
The read-only diagnostic shipped (`apply_identity_audit.py` +
`forward_apply_identity_audit`, 2.2.3) and flags `churn_suspect_models` by
comparing Forward-computed vs NetBox-stored identity keys. The root-cause fix
(stable identity keys in the apply path) needs Partner's audit output to pinpoint the
drift before it can be written.

### 5. NQE query consolidation — DEFER (smaller than believed)
Real de-dup surface is only ~2 v4/v6 variant pairs (`forward_ip_addresses_*`,
`forward_prefixes_*`); the 13 `forward_aci_*` queries are genuinely distinct.
Payoff is small; optional polish, not debt.

### 6. Optional-plugin CI coverage — CONSIDER (small, real)
28 test skips are honest optional-dependency guards (24 netbox-routing, 3
netbox-peering-manager, 1 NetBox notify hook) + 1 opt-in scale test. Confirm the
CI image installs netbox-routing and netbox-peering-manager so routing/peering
coverage cannot silently vanish; if it does not, that is a genuine (small) gap.

## Validation
Not applicable — no code changes. The audit basis is recorded in the companion
`2026-07-03-tech-debt-cleanup.md` plan.

## Rollback
Not applicable — documentation only.

## Decision Log
- Record rather than execute: every item is project-sized or blocked; ripping any
  out immediately before a release trades real risk for cosmetic gain.
- Explicitly mark #4 BLOCKED on Partner's field data and #6 as the one small,
  genuinely-actionable follow-up, so the next pass can pick it up without re-deriving.
- The `_impl` + thin public-module seam and the inert TURBOBULK/PARQUET_BULK enum
  members are intentional and stay; the audit found no dead code, markers, commented
  blocks, or version shims to remove.
