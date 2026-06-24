# Bulk Branch Merge — Large-Dataset Ingest Redesign (2026-06-22)

## Goal

Make large syncs (Blake/ADP: ~1.04M changes/sync) survive on NetBox 4.6.3 +
netbox-branching 1.1.0 by removing the dominant scale wall: the per-change merge
replay. Keep netbox-branching as the engine (hard requirement). No assumptions —
grounded in the real 1.1.0 source and Blake's actual plan_preview volumes.

## Ground truth (verified, not assumed)

- netbox_branching 1.1.0 = schema-per-branch (provision copies data into a new
  Postgres schema; 1.1.0 parallelizes the copy) + per-change merge replay
  (`merge_strategies/iterative.py` and `squash.py` both apply per-object;
  `models/branches.py:601`).
- The plugin owns its own merge loop (`utilities/merge.py`), so the merge
  mechanism is ours to change while still using branching.
- Blake's sync: 1,043,183 changes / 163 shards; dcim.interface 539k, macaddress
  286k. Per-change replay of ~1M changes is the wall (interface's single 539k
  shard guarantees a merge timeout).
- The framework's `SquashMergeStrategy` already computes net per-object state
  (`CollapsedChange`) and FK-correct ordering (`_order_collapsed_changes`,
  with cycle splitting) — reusable.

## Constraints

- netbox-branching stays the engine (hard requirement); NetBox 4.6.3 +
  netbox-branching 1.1.0 only.
- Preserve correctness invariants: cross-model FK dependency order, coalesce
  identity, MPTT `_depth`, delete safety, partial-failure isolation, idempotent
  resume.
- No customer identifiers in the diff/tests/docs.

## Approach

Bulk-ify the plugin-owned merge loop instead of replacing branching. Reuse the
framework `SquashMergeStrategy` to collapse a branch's ObjectChanges into net
per-object state with FK-correct ordering, then apply net CREATEs through
`bulk_create` batched per model. Route MPTT/tree models and UPDATE/DELETE to the
framework's per-object `ObjectChange.apply` (correctness). Make the bulk path
resume-idempotent by skipping pks already in main before deserialize/validate,
and isolate constraint violations per-row.

## Touched Surfaces

- `forward_netbox/utilities/bulk_merge.py` (new) — collapse + bulk apply.
- `forward_netbox/utilities/merge.py` — wire bulk merge + per-object fallback.
- `forward_netbox/utilities/single_branch_executor.py` (new) — one branch/sync.
- `forward_netbox/utilities/sync_orchestration.py` — route + default backend.
- `forward_netbox/choices.py` — `SINGLE_BRANCH` execution backend.
- `forward_netbox/tests/test_bulk_merge.py` (new) — provisioned-branch tests.

## Bundled changes

1. **`utilities/bulk_merge.py`** — `bulk_merge_changes()`: collapse a branch's
   ObjectChanges to net per-object state (reusing squash's collapse + ordering),
   then apply net CREATEs via `bulk_create` batched per model, instead of
   ~1M individual `ObjectChange.apply()` saves. UPDATE/DELETE and MPTT/tree
   models fall back to the framework's per-object apply (deletion-aware payloads,
   ProtectedError, tree `_depth` recompute). Resume-idempotent: skips pks already
   present in main before deserialize/validate. Per-object isolation on
   `IntegrityError` so one bad row can't fail the batch.
2. **`utilities/merge.py`** — `merge_branch` now calls `bulk_merge_changes` with
   an `apply_one` per-object fallback (savepoint→`transaction.atomic`) that wraps
   the framework `ObjectChange.apply`; keeps issue isolation, progress/heartbeat,
   `_clean`, and branch status transitions.
3. **`tests/test_bulk_merge.py`** — real provisioned-branch integration tests:
   net creates apply in one `bulk_create` while MPTT routes per-object;
   re-merge is idempotent (existing pks skipped, no duplicate-pk, no fallback).

## Tradeoff (documented)

Bulk-created rows do not emit per-object main-side `core.ObjectChange` (bulk_create
skips signals). The branch retains the full change record, and the plugin's
execution ledger records totals — acceptable for Blake's single-writer
auto-merge. UPDATE/DELETE/MPTT (per-object path) still emit main ObjectChanges.

## Validation

- Full plugin suite: 1197 tests, 0 failures, 26 skipped — on NetBox 4.6.3 +
  netbox-branching 1.1.0 (rebuilt dev env), with `SINGLE_BRANCH` as the default
  backend.
- New bulk-merge integration tests pass (bulk batching + MPTT split + idempotent
  resume). New single-branch executor test passes (one branch, all rows merged).
  Existing merge-orchestration tests unchanged.

## Rollback

Revert this commit; `merge_branch` falls back to the prior per-change loop (kept
in git history). `bulk_merge.py` is additive.

## Decision Log

- **Keep branching, change the merge** — branching is a hard requirement; the
  merge loop is plugin-owned, so bulk-ifying it is the highest-leverage,
  lowest-blast-radius scale fix.
- **Reuse SquashMergeStrategy internals** — collapse + FK ordering + cycle
  splitting are correctness-critical and already battle-tested upstream.
- **MPTT stays per-object** — bulk_create bypasses MPTT tree recompute (#531).

## Phase 2 — One branch per sync (BUILT)

`forward_netbox/utilities/single_branch_executor.py` (`ForwardSingleBranchExecutor`,
now the default `execution_backend`): provisions exactly ONE branch per sync,
stages every dependency-phased plan item into it (effectively unbounded budget,
so no size-sharding — the 10k cap is retired), then bulk-merges the single
branch once via `merge_branch`/`bulk_merge_changes`. Collapses Blake's 163
schema-copy provisions to 1. Reuses the proven `run_item_in_branch` staging path
and the framework's branch lifecycle. Wired in `utilities/sync_orchestration.py`
(`SINGLE_BRANCH` is the default; `BRANCHING` per-shard and `FAST_BOOTSTRAP`
direct-to-main remain opt-in via the `execution_backend` parameter). Tests:
`tests/test_bulk_merge.py::SingleBranchExecutorTest` (one branch provisioned,
all rows merged to main, `baseline_ready`).

### Critical finding — bulk_create into a branch records NO changes

netbox_branching tracks a branch's changes via NetBox's **signal-based** change
logging (`record_change_diff` is a post_save receiver on `ObjectChange`; the
framework has no DB triggers and no bulk-capture). `bulk_create`/`bulk_update`
fire no `post_save`, so rows written into a branch schema in bulk produce **zero
ObjectChanges** — and the merge (which replays the branch's ObjectChanges)
silently drops every bulk-staged row. Verified empirically (adapter staging →
15 ObjectChanges merged; bulk staging → 0). Therefore branch **staging must be
per-object** (adapter); the single-branch executor forces `enable_bulk_orm=false`
for staging and recovers speed on the **merge** side (bulk_create into main is
safe — main needs no per-row branch tracking). The legacy per-shard
`ForwardMultiBranchExecutor` still bulk-stages into branches and so can lose
bulk_orm rows (interface/macaddress/ipaddress/site) on merge — flagged as a
separate, serious follow-up; the now-default single-branch path is unaffected.

## Phase 3 — Merge-side scale hardening (BUILT + scale-validated)

A multi-agent scale probe + a real 50k/20k run exposed that the single-branch
merge, with the 10k cap gone, hit scale walls the cap used to hide. Fixed in
`bulk_merge.py`:
- **O(V²) ordering → O((V+E) log V)** (`_order_collapsed_changes_fast`). The
  framework `_dependency_order_by_references` rescans all remaining nodes per
  layer and discards the processed key across all nodes — O(V²) *even with no
  edges* — so a single 539k-interface batch would hang for hours. Reuses the
  framework's cycle-split + FK-graph build + signal; replaces only the sort with
  a heap-backed Kahn using a reverse index from `depends_on`. This is the
  completion blocker fix: interface-scale merges now terminate.
- **Sub-batched flush** (`BULK_MERGE_FLUSH_THRESHOLD=5000`): CREATE batches flush
  every 5k rows, each in its own transaction. Bounds peak RAM, transaction/lock
  duration, and the existence `pk__in`; committed sub-batches are resume
  checkpoints. (Was: whole 539k model in one list + one transaction → multi-GB
  RAM, all-or-nothing rollback.)
- **Batched skip-missing** (`_skip_updates_missing_in_main_batched`): groups
  collapsed UPDATEs by model with chunked `pk__in` instead of one `.exists()` per
  UPDATE (was N+1 = hundreds of thousands of round-trips on re-sync).
- **Streamed input**: collapse consumes `changes.iterator()` so the ~1M-row
  ObjectChange result set is never fully materialized.

Scale validation (`tests/test_bulk_merge_scale.py`, gated by `FORWARD_SCALE_TEST`,
run at 20k): NO silent loss, idempotent re-merge, **4 flushes ≤5000** (sub-batching
confirmed), peak RSS 463 MiB (bounded), merge 3.0 ms/row (~50 min/1M), staging
8.4 ms/row (~140 min/1M). Verdict: merge is now scale-SAFE and terminating;
**staging (per-object, forced adapter) is the remaining dominant cost** — Phase 4.

Gap-closing tests added after the flat create-only scale run (both fast,
non-gated, permanent regression guards):
- `test_bulk_merge.py::OrderingComplexityTest` — proves `_order_collapsed_changes_fast`
  orders a 50k-deep FK chain topologically AND a 50k no-edge batch, both in
  seconds (O(V²) cannot), and raises on an unbreakable cycle.
- `test_bulk_merge.py::SkipMissingBatchedTest` — proves `_skip_updates_missing_in_main_batched`
  SKIPs missing-in-main UPDATEs, keeps present ones, and runs in `ceil(n/chunk)`
  queries (`assertNumQueries`), guarding against the N+1 regression.

## Phase 4 — Bulk staging into the branch (BUILT + scale-measured)

The dominant remaining cost was forced per-object adapter staging (~140 min/1M),
required because `bulk_create` fires no post_save so the branch recorded no
ObjectChange and the merge dropped the rows. Phase 4 fixes it at the source:

- `apply_engine_bulk.emit_branch_object_changes(created, updated)` — after each
  bulk write, build each row's ObjectChange via the model's own
  `to_objectchange` (so `postchange_data` is exactly `serialize_object` output —
  the inverse the merge's `deserialize` round-trips) and `bulk_create` them
  **`.using(branch.connection_name)`** (core.ObjectChange is not branchable, so
  the router would otherwise write it to main — verified the framework targets
  the branch connection the same way, branches.py:676/743). `ChangeDiff` is not
  required by the plugin merge (it reads only ObjectChange) and Blake auto-merges
  without review, so it is intentionally not emitted.
- Wired into every bulk path: simple-models, macaddress, interface, ipaddress
  (CREATE emitted; existing rows `snapshot()` before mutation so UPDATE
  ObjectChanges carry correct prechange). VirtualChassis (two-phase, low volume)
  defers to the adapter under a branch; tree models (devicerole/platform/prefix)
  already save per-object so they track natively. The IntegrityError isolate
  path also saves per-object (tracked), so emission only runs on the bulk-success
  path.
- `single_branch_executor` now sets `enable_bulk_orm="true"` (was forced
  `"false"`): bulk staging is merge-safe now that tracking is synthesized.

Scale-measured (gated, 20k): bulk staging **2.23 ms/row (~37 min/1M)** vs the
per-object **8.4 ms/row (~140 min/1M)** — ~3.8x faster — with zero silent loss
(every row tracked, all merged, no per-object fallback). Combined 1M bootstrap
now ≈ 84 min (staging + merge) vs ~196 min pre-Phase-4. Tests:
`test_bulk_merge.py::Phase4BulkStageTest` (round-trip: bulk-stage → N branch
ObjectChanges → merge lands all in main with same pks, no fallback);
`test_bulk_merge_scale.py::...test_bulk_stage_is_fast_and_lossless` (the timing
gate). Full suite 1204 green.

## Phase 4 staging micro-optimization (done, measured)

Profiled the per-row staging floor and cut it ~2x more (gated 20k, dcim.site):
- **Batch tag prefetch** in `emit_branch_object_changes`: `serialize_object`
  resolves tags via `obj.tags.all()` (one query per row); `prefetch_related_objects`
  on the chunk makes it one query per chunk. 2.23 → 1.70 ms/row.
- **Skip `validate_unique`/`validate_constraints` on bulk CREATEs** (the
  existing-object lookup already proved the identity absent; a real violation
  still surfaces via the `bulk_create` IntegrityError → per-row isolate). Applied
  to simple-models + interface + macaddress + ipaddress. 1.70 → **1.26 ms/row**.
- Emit is **chunked** (`EMIT_OBJECT_CHANGE_CHUNK`) so the ObjectChange list never
  grows with model row count (bounded memory at 539k).
- Tried batching the JSON serialize itself — no measurable gain (full_clean, not
  serialization, was the cost), so reverted to keep `to_objectchange` (exact
  format, lower risk).

Cumulative staging: **8.4 → 1.26 ms/row (~6.7x)**, ~140 min → **~21 min/1M**.
Combined 1M bootstrap now ≈ 68 min (staging ~21 + merge ~47). Full suite 1205
green.

## Follow-on (next steps of the redesign)

- Merge is now the larger half (~2.8 ms/row, ~47 min/1M) — the per-object
  deserialize in `bulk_merge._deserialize` is the next target if more speed is
  needed.
- Full end-to-end UPDATE-staging-at-scale variant (the UPDATE→apply_one merge
  path and batched skip-missing are covered by unit tests; per-object snapshot
  emission on the bulk UPDATE path is exercised by Phase4BulkStageTest only at
  small scale).
- **Watermark deltas** — first sync bootstraps + stamps baseline; later syncs
  apply only Forward deltas, making steady-state O(actual change).
- **Fix or retire bulk-into-branch in `ForwardMultiBranchExecutor`** — same
  signal-tracking gap as above; force adapter staging there too, or deprecate
  the per-shard backend now that single-branch is the default.
- **Watermark delta routing** — first sync bootstraps + stamps baseline; later
  syncs apply only Forward deltas (machinery already exists). Makes steady-state
  O(actual change) and shrinks per-object staging to only changed rows.
- Sub-batch the giant single-model stage writes (interface 539k).
