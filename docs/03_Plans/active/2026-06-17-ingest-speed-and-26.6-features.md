# Ingest Speed Hardening and Forward 26.6 Feature Adoption

## Goal

Make Forward → NetBox ingestion as fast as NetBox allows while keeping 100%
production quality (parity, change visibility, Branching semantics). Adopt the
Forward 26.6 capabilities the plugin now requires but does not yet use.

The dominant cost is NetBox per-object write overhead on high-cardinality
models. The fetch path is already concurrent and is not the bottleneck. This
plan attacks the write path first, then trims fetch latency, then adds 26.6
feature surface.

## Reality Map (evidence)

- Fetch is concurrent: `ThreadPoolExecutor`, default 10 / max 16 workers
  (`forward_netbox/utilities/query_fetch_execution.py:567`,
  `forward_netbox/utilities/forward_api_impl.py:32`). Identity caches remove FK
  lookup N+1 (`forward_netbox/utilities/sync_primitives.py:185`). Not the
  bottleneck.
- Two apply engines:
  - Bulk ORM (`bulk_create`/`bulk_update`, batch 1000) covers only 9
    low-cardinality reference models and is **off by default**
    (`forward_netbox/utilities/apply_engine_decision.py:20`, reason
    `bulk_orm_disabled_by_default`; engine in
    `forward_netbox/utilities/apply_engine_bulk.py:304`).
  - Per-row adapter runs `full_clean()` + `save()` per object
    (`forward_netbox/utilities/sync_primitives.py:108`) for every
    high-cardinality model — device, interface, ipaddress, cable, taggeditem,
    inventoryitem, module (`ADAPTER_REQUIRED_MODELS`,
    `forward_netbox/utilities/apply_engine_decision.py:221`). These are the
    team's own top-priority perf models
    (`BULK_ORM_PERFORMANCE_IMPACT_PRIORITY:209`).
- Per row in the adapter path you pay: `full_clean()` (validators + uniqueness
  DB hits) + `save()` → `post_save` → `ObjectChange` changelog insert + webhook
  / event-rule evaluation + cable/scope recalcs.
- No general signal/changelog suppression during ingest — only narrow
  merge-side-effect suppression
  (`forward_netbox/utilities/ingestion_merge.py:44`).
- Async NQE polling is a fixed 1.0s sleep, max 1200 polls
  (`forward_netbox/utilities/forward_api_impl.py:42`, sleep at `:1299`); trigger
  response already short-circuits on `COMPLETED` (`:1285`).
- ndjson result is buffered: `response.text` then `splitlines`
  (`forward_netbox/utilities/forward_api_impl.py:1167`).
- A faster backend already exists but is opt-in: `FAST_BOOTSTRAP` auto-enables
  bulk ORM + max concurrency; default backend is `BRANCHING`
  (`forward_netbox/choices.py:126`, `forward_netbox/forms.py:1101`).

## Constraints

- Forward 26.6 is the baseline. No backwards compatibility.
- Preserve NetBox object-change visibility expected by operators and Branching
  review (the `object_change_tracking_parity` gate). Do **not** suppress the
  changelog itself.
- Every apply-engine change must pass the existing parity gates before default
  enablement: validation parity, change-tracking parity, Branching semantics
  parity, row-issue parity, runtime non-regression
  (`apply_engine_decision.py:45`).
- Keep customer data, credentials, network IDs, and live execution keys out of
  docs and tests. Live validation is local (the ORG network, via the
  untracked `.env`).
- No change to the public `ForwardClient.run_nqe_query` contract.

## Touched Surfaces

- `forward_netbox/utilities/apply_engine_decision.py` (default-on safe bulk set)
- `forward_netbox/utilities/apply_engine_bulk.py` (ipaddress spec; validation
  trim)
- `forward_netbox/utilities/sync_primitives.py` (update batching; targeted
  validation)
- `forward_netbox/utilities/sync_execution.py` (signal-suppression scope around
  the apply loop)
- `forward_netbox/utilities/ingestion_merge.py` (reuse signal-suppression
  helper)
- `forward_netbox/utilities/forward_api_impl.py` (adaptive poll backoff; ndjson
  streaming; reachability trigger)
- `forward_netbox/forms.py`, `docs/01_User_Guide/configuration.md` (any new
  params / default changes)
- Tests: `forward_netbox/tests/test_forward_api.py`,
  `test_sync*.py`, apply-engine tests, plus runtime evidence under
  `docs/03_Plans/evidence/`

## Approach

Ordered by ROI. Each tranche is independently shippable.

### Tranche A — safe quick wins (low risk, ship first)

1. **Default-on bulk ORM for the proven-safe set.** ✅ DONE (already auto-enabled)
   `_bulk_orm_enabled_state` returns `(True, True)` when `enable_bulk_orm` is
   unset — the 9 safe models already use bulk by default. No code change needed.

2. **Adaptive async-NQE poll backoff.** ✅ DONE
   `_wait_for_nqe_async_completion` uses `min(ceiling, 0.1 * 2**poll_index)`.
   Fast queries return in ~0.1s; slow ones plateau at the configured ceiling.

3. **Stream ndjson results.** ✅ DONE
   `_parse_nqe_lines` iterates via `io.StringIO` instead of `splitlines()`,
   avoiding a full-body string copy for large result sets.

### Tranche B — fetch/apply pipelining and update batching (medium)

4. **Batch updates on the adapter path.** ❌ REVERTED — wrong approach.
   Batching updates via `bulk_update` skips Django `post_save`, which skips
   `ObjectChange` creation and therefore removes the update from Branching diff
   review. **Updates must go through the Branching framework** (per-row
   `save()` → signals → changelog). Bulk fast-path writes are acceptable only
   for **initial load** (creates / `FAST_BOOTSTRAP`), not for updates.
   The correct way to cut update-side load is to **fetch fewer rows** (NQE
   diffs — see "Server Load Reduction" below), never to bypass per-row writes.
   Guarded by `test_apply_dcim_interface_update_records_object_change`.

5. **Suppress webhook / event-rule signals during the apply loop.** ✅ DONE
   `suppress_ingest_side_effect_signals()` in `ingestion_merge.py` is the
   general context manager; both `sync_execution.py` (apply loop) and the merge
   phase delegate to it. Disconnects `assign_virtualchassis_master`,
   `sync_cached_scope_fields`, and `notify_object_changed` for the duration.

6. **Targeted validation instead of full `full_clean()`.** ✅ DONE (both bulk engine paths)
   `apply_engine_bulk.py` UPDATE path in both `bulk_orm_apply_simple_models` and
   `bulk_orm_apply_tree_models` now calls `clean_fields()` + `clean()` instead of
   `full_clean()` for existing objects, skipping the DB-hitting `validate_unique()`
   and `validate_constraints()`. CREATE path keeps `full_clean()` since new objects
   need uniqueness validation. Covered by `test_bulk_orm_update_uses_targeted_validation_not_full_clean`
   and `test_bulk_orm_create_uses_full_clean`.

### Tranche C — extend bulk coverage up the priority list (higher effort)

7. **Promote `ipam.ipaddress` to bulk ORM.** ⛔ BLOCKED — deferred to next cycle.
   `apply_ipam_ipaddress` (`sync_ipam.py:383`) assigns a GenericFK
   (`assigned_object_type` + `assigned_object_id`) pointing at a
   `dcim.Interface` PK, performs device→interface dependency resolution, and has
   skip logic for unresolved parents. These cannot be expressed as a simple
   field-set spec without a dedicated spec that replicates the dependency graph.
   Required: a new `BULK_ORM_SPEC` entry with pre-fetched interface/device maps,
   GenericFK resolution pass, and per-row failure isolation matching the current
   adapter behavior. Risk is high relative to the remaining models; deferred
   until B4 batch-update path is validated in production.

8. **Promote `dcim.interface` to bulk ORM.** ⛔ BLOCKED — documented blocker.
   LAG parent ordering, cable endpoint side-effects, and scope recalculation
   require careful sequencing that the current bulk engine does not model.
   Documented at `apply_engine_decision.py:267`. Hardest; do last.

### Tranche D — Forward 26.6 features

9. **Async advanced-reachability trigger (FWD-53559).** ✅ DONE (client layer)
   `ForwardClient.trigger_snapshot_reachability(network_id, snapshot_id)`
   POSTs to `/networks/{id}/snapshots/{id}/reachability`, extracts `jobKey`,
   and polls `_wait_for_reachability_completion` with the same exponential
   backoff as NQE async. Terminal states: `COMPLETED`/`DONE`/`READY` (success),
   `FAILED`/`ERROR` (raises `ForwardClientError`). 4 tests added covering happy
   path, polling, failure, and missing-arg validation. Wiring into the sync
   runner as a pre-ingestion gate is a follow-on (depends on the roadmap doc
   `2026-05-13-forward-roadmap-intent-path-blast-radius-predict.md`).

10. **Incremental ndjson → bulk pipeline.** ⏳ DEFERRED — depends on B4.
    Once B4 batch-update is validated in production, rows from ndjson streaming
    can feed directly into the update queue without a full intermediate list,
    overlapping fetch and apply for large result sets.

## Server Load Reduction (Forward API / NQE)

Distinct goal from write speed: minimize load on the Forward server. The write
path (NetBox-side) and the read path (Forward-side) are optimized separately.
Updates write through the Branching framework per-row (for change review); the
read-side savings come from fetching less and calling the API less.

Levers and current state:

1. **Parameterized, committed query_ids.** ✅ Present.
   Specs carry `query_id` / `resolved_query_id` + `merged_parameters`
   (`query_registry.py:41-83`). Raw query text is resolved to a committed
   `queryId` via the org repo. Required so NQE diffs are eligible.
   - TODO: audit all built-in maps so every query resolves to a `query_id`
     (any raw-text-only query disqualifies that model from diffs → forces full
     fetch every sync).

2. **Async NQE executions.** ✅ Done.
   `/nqe-executions` trigger → backoff poll → ndjson result.

3. **NQE diffs for updates.** ✅ Wired, verify coverage.
   When a baseline ingestion exists and the spec has `run_query_id`,
   `sync_execution.py:130` runs `run_nqe_diff` against
   `/nqe-diffs/{before}/{after}` and fetches only changed rows. Diff rows split
   into upserts + deletes. Falls back to full only when no `query_id` or no
   baseline.
   - This is the primary update-side load reduction. Confirm on live ORG that
     re-syncs actually take the diff path (not full) for the high-cardinality
     models.

4. **Only call the API when necessary.** ⚠️ Gap.
   Read cache (snapshot metrics, org queries) and latest-processed catch-up
   exist. But when the current snapshot equals the last successful baseline
   snapshot, `sync_execution.py:116` still runs **full** queries for every
   model — re-fetching unchanged data.
   - TODO: when `current_snapshot == latest_baseline.snapshot` and the sync is
     not an explicit/forced re-run, **skip query execution entirely** (no-op
     sync, mark completed). Gate behind an `adhoc`/force flag so manual
     re-syncs still work. Biggest single API-load win for scheduled syncs on a
     stable snapshot.
   - TODO: avoid re-fetching the snapshot list + metrics when already cached for
     the run.

## Validation

- `python -m pytest forward_netbox/tests/test_forward_api.py` (poll backoff,
  ndjson streaming, reachability trigger request shape).
- `invoke test` (full Django suite in the NetBox 4.6.2 container) for apply-
  engine + sync parity.
- Apply-engine parity gates per `BULK_ORM_PARITY_CHECKLIST` for every model
  promoted to bulk (create/update/delete/validation-failure/row-issue/
  dependency/change-tracking parity).
- `invoke lint`, `invoke check`, `invoke docs`.
- **Live ORG runtime evidence** (local, via the untracked `.env`):
  capture before/after sync wall-clock per tranche and store under
  `docs/03_Plans/evidence/` (no creds, no network internals in the file). A
  tranche only defaults-on after it shows equal-or-better runtime
  (`runtime_non_regression` gate).

## Rollback

- Each tranche is an isolated commit; revert the commit.
- Bulk-default and validation-trim changes are config-flag guarded — flip the
  flag off without code revert.
- No persisted migration state is introduced by Tranches A, B, D(9). Bulk
  promotion (C) writes the same NetBox objects via a different code path, so
  rollback is code-only; no data cleanup.

## Decision Log

- Do not suppress the NetBox changelog during ingest — Branching review depends
  on it. Suppress only webhook/event-rule side effects.
- Do not convert device/interface creates to bulk in the first pass — relationship
  side-effects (LAG, cable, scope) carry blockers documented in
  `apply_engine_decision.py`. Batch the update case first; full bulk later.
- Adaptive poll backoff over a smaller fixed interval: a smaller fixed interval
  raises request volume against Forward for slow queries; backoff gets fast-query
  latency without punishing slow ones.
- ipaddress before interface for bulk promotion: higher impact-to-risk ratio.
  However, GenericFK complexity in `apply_ipam_ipaddress` blocks a clean bulk
  spec; deferred until B4 batch-update proves stable in production.
- **Updates go through the Branching framework, always.** Per-row `save()` so
  `post_save` fires and `ObjectChange` records the update for Branching diff
  review. Never batch updates via `bulk_update` (it skips signals → no
  changelog). Bulk fast-path is for initial load (creates / FAST_BOOTSTRAP)
  only. B4 (batch updates on the adapter path) was implemented then reverted
  for this reason. Update-side load is reduced by fetching fewer rows (NQE
  diffs), not by changing how writes happen.
- B6 targeted validation applied to bulk engine UPDATE path only; CREATE path
  keeps full_clean() to catch uniqueness violations on new objects.
- D9 reachability trigger implemented at the client layer; sync-runner wiring
  deferred to the intent/path roadmap work.
- Keep `FAST_BOOTSTRAP` as a distinct backend rather than merging its behavior
  into `BRANCHING` default — operators rely on Branching review for production
  change control.
