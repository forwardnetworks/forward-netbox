# Scale Hardening Remaining Work

## Goal

Finish the performance and recovery work unlocked by the execution ledger.

The current ledger gives every large Branching run a durable run/step boundary.
The remaining work should use that boundary to reduce repeated work, add
operator recovery controls, and prepare for future high-throughput apply
engines without changing the NQE source-of-truth contract.

## Execution Status

Completed for the current scale-hardening tranche on 2026-05-16.

The implementation now satisfies the release-scope completion definition below:
Branching orchestration is ledger-first, operator diagnostics are available from
native NetBox surfaces, per-model fetch/apply decisions are visible, row-level
and recovery support bundles are sanitized and durable, stale/duplicate/retry
failure modes have a deterministic release gate, and the full local CI gate
passes.

The items still called out as future work are intentionally **not** hidden:
removing `_branch_run` entirely, proving deeper live query pushdown with Forward
runtime data, adding a faster non-adapter apply engine, and adding destructive
Docker worker-kill injection all remain explicitly deferred architecture
targets. They are no longer blockers for this tranche because the current code
keeps compatibility reads/writes only as an upgrade/read-through safety window,
documents the proof required before removal, and exposes enough Health/support
evidence to operate the current system without those future engines.

## Current Baseline

Already implemented in the current tranche:

- `ForwardExecutionRun` persists run-level state for Branching execution.
- `ForwardExecutionStep` persists one stage step per planned Branching shard.
- Stage jobs claim a ledger step before work starts.
- Duplicate stage jobs exit if the ledger step is already terminal.
- Existing sync-parameter branch state remains only as a compatibility/display
  cache; active orchestration now prefers the execution ledger when a run
  exists.
- Stage enqueue and merge handoff now synthesize from the ledger when
  compatibility branch state is absent, and prefer the active execution run
  when both exist.
- Progress/failure and retry helpers now update the active execution run and
  step records first, falling back to compatibility JSON only when no execution
  run exists.
- Runtime phase updates now also persist to the active execution run first,
  falling back to compatibility JSON only when no execution run exists.
- Failure reconciliation now uses the synthesized ledger display state when
  compatibility branch JSON is absent or stale.
- Fresh planned Branching runs now create their execution-run records before
  applying shards, including direct/non-job execution paths, without first
  writing a compatibility branch-state cache.
- Plan-item updates now require an active execution run before touching ledger
  step state, so completed historical runs cannot be mutated by later sync
  planning or fallback bookkeeping.
- The seeded UI harness fixture now resolves the execution run from ledger
  state instead of injecting `execution_run_id` into compatibility JSON.
- Ingestion log export includes the latest execution run bundle.
- Execution runs and steps are exposed through native NetBox list/detail views.
- Execution run and step API endpoints expose read-only ledger state.
- Execution runs support native reconcile, retry-current-step, requeue-merge,
  and support-bundle actions.
- Execution runs persist bounded reconciliation event history so support bundles
  show what reconcile observed and changed.
- Execution run support bundles and detail views include a ledger-derived
  recovery recommendation.
- Support bundle regressions now cover cleanup, later runs, and upgrade-from-
  old-branch-state cleanup proofs.
- Reconciliation now converts stale stage workers with a recorded branch into an
  explicit discard-and-retry recommendation, and stale merge workers with a
  recorded branch into a requeue-merge recommendation.
- Execution steps persist fetch contract fields: mode, key family, NQE
  parameters, and NQE column filters.
- Execution steps persist row-application counters: attempted, applied, skipped,
  and failed rows.
- Late-stage shard retries pass shard scope into query planning, including
  missing-JSON resumes where the persisted shard scope is reconstructed from
  `ForwardExecutionStep`.
- Single-device shards use native NQE column filters.
- Parameterized shard-capable NQE maps receive the standard
  `forward_netbox_shard_*` parameters, with fallback to full model fetch if the
  backend rejects them.
- Shard-parameter built-in NQE maps guard primary `network.devices` iterators
  with `forward_netbox_shard_keys`; reciprocal `peer_device` inference lookups
  remain global so routing and OSPF inference stays correct across shard
  boundaries.
- Large native column-filter shard fetches now partition oversized
  `EQUALS_ANY` batches into multiple native query calls before falling back to
  a full-model fetch.

Important remaining limitation:

- Multi-device device-scoped shards now use native NQE `EQUALS_ANY` column
  filters to avoid fetching unrelated result rows. Deeper query-pushdown
  parameterization remains future work if Forward query runtime, rather than
  result pagination/transfer, is the bottleneck.
- IPAM shard scoping still needs a model-specific contract because IPAM identity
  is not always device-key based. **Partially done: `ipam.prefix`,
  `ipam.vlan`, and `ipam.vrf` now use broad native NQE column filters when their
  shard keys expose stable filter columns; exact shard membership is still
  enforced by the local safety filter.**
- The apply-engine boundary exists and records the active engine in execution
  steps and ingestion model results. The only implemented engine is `adapter`,
  so current apply behavior still uses the proven row-by-row adapter path.

## Roadmap Summary

The large-sync architecture should converge on these durable targets. They are
the remaining items that matter for scale, speed, reliability, and
self-service operation.

## Executive Remaining Work

The project is now pointed in the right direction for large customer
deployments, but the scale architecture is not "done" until these items are
either implemented or explicitly deferred with evidence:

1. **Ledger-only orchestration.** The execution ledger exists and is already
   useful, but `_branch_run` compatibility state still needs to stop being an
   active orchestration input.
2. **Per-model fetch contracts.** Every model needs a declared fetch contract
   that says whether it supports shard-scoped NQE fetch, diff fetch, local
   safety filtering, or full fallback.
3. **True query-side shard reduction where Forward supports it.** Current native
   column filters reduce returned rows for supported shard shapes. The next
   speed target is NQE query pushdown or hash/bucket partitioning that avoids
   materializing unrelated rows in the first place.
4. **Apply-engine capability reporting.** The `adapter` engine remains the only
   safe general engine today. Before adding `bulk_orm`, the plugin should expose
   why each model stayed on `adapter` and what proof would be required to move
   it.
5. **Formal chaos/scale gate.** Interrupted workers, stale branches, duplicate
   callbacks, partial merges, and late-shard retries need a repeatable local
   release gate, not only unit coverage.
6. **Self-service support workflow.** Health, support bundles, recovery
   recommendations, and log export now answer the main local-state, live-check,
   and recovery questions from native NetBox surfaces. The remaining work is to
   keep broadening those surfaces as new execution and performance engines are
   added.
7. **Branch alignment.** Keep one shared code branch for supported NetBox
   versions. Differences should be limited to runtime capability, not product
   behavior.
8. **Immutable run evidence.** Support bundles should not depend only on the
   current sync JSON state. Long runs need durable run/step evidence that can be
   exported after completion, failure, cleanup, or compatibility-state removal.
9. **Concurrency-safe state transitions.** Once the execution ledger is the
   active source of truth, run/step advancement should use explicit claim,
   version, or row-lock semantics instead of best-effort sync-parameter writes.

These items are intentionally framed as architecture outcomes rather than file
tasks. Implementation can happen in multiple tranches, but each tranche should
move one of these outcomes measurably forward.

## Open Architecture Register

This is the current high-level register for the work still needed to make the
plugin self-service at very large scale. It is intentionally separate from the
detailed workstreams below so release and planning conversations can start from
one short table.

| Area | Current state | Long-term target | Next concrete tranche |
| --- | --- | --- | --- |
| Ledger orchestration | Execution runs/steps exist and most recovery/display paths can read ledger state. `_branch_run` still exists as a compatibility cache, but fresh planned runs, queue handoff, merge continuation, stage-worker resume, runtime phase updates, progress/failure reporting, retry handling, failure reconciliation, sync enqueue continuation, late-shard scope handoff, and sync support-bundle export after cleanup/later-run handoff now prefer ledger state whenever a run exists. Public pending/merge gates now ignore stale compatibility JSON once a real execution run is present. Completed historical ledgers are not treated as active plan state for new runs or fallback plan-item updates. Support export after cleanup and Playwright/API operator actions is covered. | `ForwardExecutionRun` and `ForwardExecutionStep` are the active control plane; `_branch_run` is only upgrade/read-through compatibility. | Prove the one-release compatibility window across upgrade/cleanup scenarios, then retire active JSON writes. |
| State transitions | Stage, merge, retry, discard, and finalize paths have targeted idempotency guards and transaction-backed simultaneous-worker coverage. | Every state transition is claimed under a ledger row lock or equivalent lease and duplicate callbacks are deterministic no-ops. | Keep extending the stress matrix as new recovery transitions are added. |
| Shard fetch | Every supported model reports a fetch contract with fetch mode, key family, schema contract, local safety-filter guarantee, and fallback reason. Device-scoped and selected IPAM shards use safe native NQE column filters. A schema-parity regression now exercises every supported model through the fetch path, while representative regressions prove shard-scoped `dcim.interface` and `ipam.prefix` fetch preserve row shape and `dcim.site` proves the safe full-fetch fallback path still preserves row shape under local filtering. | Every model has an explicit fetch contract: shard-safe filter, NQE pushdown parameter, hash/bucket strategy, or full fallback with reason. | Extend schema/parity coverage into live fixtures and hash/bucket contracts for models without stable natural filters. |
| Query pushdown | Column filters reduce returned rows for supported shapes, oversized `EQUALS_ANY` shard filters now partition into multiple native query calls, and shard-parameter built-in NQE maps scope primary device iterators with `forward_netbox_shard_keys`. Cross-device peer inference intentionally remains global. | Built-in NQE maps support optional pushdown parameters where Forward can avoid materializing unrelated rows. The current query language evidence does not yet show a supported hash/mod primitive for a safe bucket contract, so bucketed pushdown stays blocked until the Forward-side query surface proves an equivalent deterministic expression. | Profile the slowest enabled maps from real execution metrics, validate the parameterized query path against live fixtures, and add pushdown only where the data model proves safe and output shape stays identical. |
| Apply engines | The apply-engine boundary records `adapter` and reports why faster engines are not selected. | Faster engines are capability-gated implementation details below Branching and fast bootstrap, not separate workflows. | Prove a small `bulk_orm` candidate set for simple models only, or keep it deferred with explicit blockers if native validation/change tracking cannot be proven. |
| Fast bootstrap to Branching | Fast bootstrap can seed trusted baselines, and Health now exposes structured next-run blockers for fixed snapshots, missing baselines, and raw-query maps. The first later Branching run may still perform reconciliation if runtime snapshot/query metadata does not align. | Operators can see before starting whether the next run is true diff, reconciliation, or full fallback, with map-level blockers. | Extend blocker reporting with live/pinned commit drift and latest-snapshot comparison when the operator requests an explicit live check. |
| Operator diagnostics | Health, log export, and support bundles cover most local-state and explicit live-check questions. Ledger-derived branch-run exports now label their source, execution-run bundles include sanitized linked-ingestion issue summaries without raw row payloads, and run metrics now identify the largest measured runtime phase. | A support bundle explains run state, query state, row failures, branch/merge state, and recommended action without screenshots. | Keep broadening support bundles as new fetch/apply engines land and preserve bottleneck reporting as more timing fields are added. |
| Chaos validation | `invoke scale-chaos-test` is a deterministic synthetic gate wired into CI. It now covers stale stage workers before branch creation, stale stage workers after branch creation, stale merge workers, duplicate callbacks, partial-branch discard, and late-shard retry. Destructive Docker worker-kill scenarios remain future work. | Release validation proves recovery from worker death, stale branches, duplicate callbacks, partial merges, and late-shard retry. | Add local Docker failure injection for worker hard-kill before branch, after branch, during row apply, and during merge. |
| Branch alignment | One shared product surface should cover all supported NetBox versions. | One code branch exposes the same operator workflow, with only runtime-specific capability differences. | Gate version-specific features behind explicit capability checks or optional maps. |
| Data sensitivity | Plans and tests avoid raw customer rows, network IDs, snapshot IDs, and credentials. | No durable docs, tests, logs, support bundles, or fixtures require private customer data. | Keep sensitive-content gates in release validation and add support-bundle fixture checks as export fields grow. |

Decision guardrails:

- Do not make fast bootstrap the default review path; it is a trusted-baseline
  tool for large first loads.
- Do not raise branch budgets to hide slow Branching merges. Preserve the
  operator guidance and make runtime risk visible instead.
- Do not add Python-side data normalization that diverges from NQE. Python can
  validate, apply, skip, and report, but NQE owns model-shaped data.
- Do not add a separate bulk-sync workflow. Bulk behavior belongs under the
  existing Branching and fast-bootstrap execution lanes.
- Do not remove `_branch_run` writes until old-state upgrade, missing-JSON
  recovery, support export, native UI/API actions, and scale/chaos gates prove
  ledger parity.

## Long-Term Completion Backlog

This backlog captures the larger architectural work that is intentionally still
open after the current ledger, health, support-bundle, and recovery tranches.
It should be used for future planning before adding new point features.

| Item | Why it matters | Current position | Completion signal |
| --- | --- | --- | --- |
| Ledger-only orchestration | Parameter JSON is not a strong enough control plane for multi-day, multi-worker syncs. | Ledger records drive much of recovery, display, support export, and late-shard resume planning, while `_branch_run` still acts as a compatibility cache for some handoffs. | Branching stage, merge, retry, discard, finalize, UI, API, Health, sync enqueue continuation, and support bundles all operate from ledger state after `_branch_run` is cleared; active JSON writes can enter a documented compatibility-removal window. |
| True shard-scoped NQE fetch | Retrying or staging many shards should not repeatedly pay full-model Forward query cost when the data model supports safe filtering. | Device-scoped and selected IPAM filters exist; every model exposes fetch mode, schema contract, local safety-filter guarantee, and fallback reason in Health. Shard-parameter query sources now guard primary device iterators with `forward_netbox_shard_keys`. | Every supported model declares a shard-safe NQE filter, hash/bucket parameter, or explicit full-fallback reason, with schema parity tests proving filtered and full outputs remain contract-compatible. |
| NQE query pushdown | Column filtering is useful, but some queries may still materialize large intermediate data before returning the filtered rows. | Device-scoped built-in maps now have `forward_netbox_shard_keys` guards on primary device iterators with regression coverage; peer inference remains cross-shard for correctness. Bucketed/hash pushdown remains blocked on a query-language primitive we have not yet proven in the available Forward-side query surface. | Slowest enabled maps have execution metrics, targeted pushdown parameters, live before/after proof that output shape is identical, and measured query runtime or row-volume reduction. |
| Apply-engine acceleration | Large trusted baselines need faster writes, but correctness depends on NetBox validation, object changes, Branching semantics, and issue capture. | The apply-engine boundary reports `adapter` and explains why faster engines are deferred. | A conservative faster engine is enabled only for proven simple models, or remains explicitly deferred with tests showing every model has a documented blocker. |
| Fast-bootstrap to Branching continuity | Operators need to know whether a fast baseline can seed later Branching diffs or whether one reconciliation run is expected. | Health reports fixed-snapshot, missing-baseline, and raw-query blockers. | Pre-run Health explains true diff, reconciliation, or full fallback per map, including query identity, commit pinning, baseline snapshot, and dirty-run blockers. |
| Query-library and data-file drift | Many support cases come from stale published queries, wrong binding mode, missing data-file capture, or unpinned query revisions. | Local drift classification reports raw/query-path/query-ID state plus pinned-versus-latest commit behavior; explicit live source/query/data-file exports exist. | Keep source comparison, commit-binding guidance, data-file capture status, and operator recommendations covered as Forward repository APIs evolve. |
| Self-service support bundles | Support should not need screenshots or one-off database digging to explain a failed shard. | Sync, ingestion, and execution-run bundles include sanitized run, step, job, issue, and recommendation detail. Sync support bundles now also survive cleanup plus later-run handoff while still reporting ledger provenance. | One exported bundle can diagnose merge failures, row failures, routing/plugin failures, virtual-chassis skips, IPAM skips, query drift, timeout risk, and cleanup state without raw rows or private identifiers. |
| Destructive chaos validation | Unit tests and synthetic gates do not prove recovery from killed workers or interrupted container/runtime state. | `invoke scale-chaos-test` covers deterministic stale/retry/merge/duplicate/late-shard/log-export cases, including stale stage workers before and after branch creation plus stale merge workers. | A local Docker chaos gate kills workers before branch creation, after branch creation, during row apply, and during merge, then proves recovery actions and support bundles stay coherent. |
| Capacity and database operations | Branching cost can be dominated by PostgreSQL temp schemas, ChangeDiff generation, merge cleanup, and worker timeout limits. | Health exposes timeout settings and observed shard capacity projection; docs include basic sizing guidance. | Health and docs explain planned branch count, observed runtime bottlenecks, timeout risk, database cleanup/retention expectations, and when to choose fast bootstrap over reviewable Branching. |
| Branch/version alignment | One shared code line should not diverge into different products. | One code branch is the stable surface; version-specific work is called out in this plan. | All supported NetBox versions expose the same operator workflow, with runtime-specific differences limited to capability detection and engine internals. |

Implementation rule: each future tranche should move one row from "current
position" to "completion signal" with direct tests or explicit deferral
evidence. Do not add a new sync lane, Python-side data normalization, or
customer-specific workaround to satisfy these items.

- If testing exposes a real architectural issue or a clearly better
  opportunity, pursue it in the same tranche instead of deferring it silently.

## Long-Term Alignment Assessment

The current direction is sound: NQE remains the normalization/source-of-truth
layer, NetBox native models remain the mutation boundary, Branching remains the
reviewable path, and fast bootstrap remains the explicit trusted-baseline path.
The execution ledger, support bundles, recovery actions, health tab, and
apply-engine boundary are the right foundations for scale because they turn a
long sync from an opaque job into a sequence of inspectable, retryable steps.

What is not finished is the data-plane and operator-readiness work that makes
that foundation fast and self-service at very large scale. The remaining
alignment gaps are:

- `_branch_run` compatibility state still exists beside the execution ledger.
  This is acceptable during migration, but the ledger must become the active
  orchestration source before the architecture is considered complete.
- Shard execution is only partially shard-scoped. Device-scoped and selected
  IPAM cases can use native NQE column filters, every model now reports an
  explicit fetch/fallback contract, and shard-parameter NQE maps now scope
  primary device iterators with `forward_netbox_shard_keys`. Remaining work is
  schema proof against live full-query output, live performance proof, and
  hash/bucket contracts for models without a stable natural filter.
- Fast bootstrap removes Branching diff/merge cost for trusted baselines, but
  the write path still uses the conservative adapter engine. A faster
  `bulk_orm` or future TurboBulk/parquet engine should sit under the existing
  execution lanes, not become a separate workflow.
- Query-library state is locally diagnosable and has explicit live export
  checks for repository/direct-query drift, pinned-versus-latest commit
  behavior, and optional data-file freshness.
- Chaos and scale validation are not yet a formal release gate. Unit and UI
  tests cover normal behavior, but worker death, stale branches, partial
  merges, duplicate callbacks, and late-shard retries need repeatable Docker
  failure injection before scale changes are called production-ready.
- Self-service diagnostics are improving, but the product should eventually
  answer "why is this slow?", "why did this run full instead of diff?", "what
  should I retry?", and "what bundle do I send support?" without asking the
  operator to assemble screenshots.
- Run evidence still needs a stronger lifecycle guarantee. Support bundles
  should be reconstructable from execution run/step records after successful
  completion, failed cleanup, or `_branch_run` retirement, not only while the
  current sync compatibility state still exists.
- Concurrency protection should move from compatibility JSON updates to
  ledger-owned transitions. Step claiming already points in that direction; the
  remaining orchestration paths should converge on database-backed run/step
  transitions with clear stale-job reconciliation behavior.

### Target Operating Model

| Area | Target state | Next architectural move |
| --- | --- | --- |
| Orchestration | Execution ledger is the only active source for run/step state. | Move all UI/API/recovery decisions off `_branch_run`, then deprecate active writes. |
| Fetch | Each model has a declared shard contract or explicit fallback. | Add per-model contracts, schema parity tests, and support-bundle fetch explanations everywhere. |
| Apply | Execution backend chooses review semantics; apply engine chooses write mechanics. | Keep `adapter` default, expose per-model capability/fallback reasons, and add `bulk_orm` only after native validation/change-tracking proof exists. |
| Diff baseline | Operators can see whether the next run is true diff, reconciliation, or full fallback. | Persist and expose baseline blockers by map/query/snapshot. |
| Diagnostics | Health and support bundles explain state without side effects, with explicit live checks only when requested. | Keep expanding the recommendation text and pre-run blockers as new fetch/apply engines are added. |
| Recovery | Stale or failed steps have one native recommended next action. | Finish chaos gates and make recovery recommendations part of release validation. |
| Branches | One shared code branch serves all supported NetBox versions and feature flags. | Backport product features consistently; isolate only runtime-specific capability work. |
| Evidence | Support data remains available after completion, failure, cleanup, and compatibility-state removal. | Persist enough run/step/job/branch evidence for support bundles without relying on live `_branch_run` JSON. |
| Concurrency | Multiple NetBox workers cannot advance the same step or merge path twice. | Keep extending claim/lock/version semantics to every stage, merge, retry, and finalize transition. |

## Remaining Long-Term Architecture Alignment

This is the detailed workstream breakdown for getting the project fully aligned
with the scale architecture. Items are grouped by the architectural problem
they solve, not by file. The order matters: observability and recovery stay
ahead of performance engines so failures remain debuggable while the
implementation gets faster.

The current codebase has the right major boundaries: NQE owns normalization,
NetBox adapters own native object application, Branching owns reviewable
changes, fast bootstrap owns trusted first-load seeding, and the execution
ledger owns long-run state. The remaining work is not another user-facing sync
mode. It is a set of deeper platform improvements under those boundaries so the
plugin can scale without becoming harder to operate.

### 1. Make The Execution Ledger The Only Control Plane

Current state:

- The ledger is durable and supportable, and most display/recovery paths can
  already read it.
- `_branch_run` still exists as compatibility JSON and is still written by some
  orchestration paths.

Target:

- `ForwardExecutionRun` and `ForwardExecutionStep` become the active source for
  orchestration, UI state, API actions, recovery, support bundles, and
  completion/baseline decisions.
- `_branch_run` becomes read-through upgrade compatibility only, then is retired
  after one documented release window.

Remaining work:

- Finish missing-JSON parity for every coordinator, stage, merge, retry,
  discard, finalize, health, API, and support-bundle path.
- Add upgrade fixtures for old `_branch_run` state and prove native UI/API
  actions work after compatibility JSON is absent.
- Extend row-lock/claim semantics to every transition and add simultaneous
  worker stress coverage, not only targeted duplicate-job tests.
- Make support bundles reconstructable from ledger records after completion,
  cleanup, failed reconciliation, and later runs.

### 2. Turn Shard Fetch Into A Per-Model Contract

Current state:

- Device-scoped shards and selected IPAM models can use native NQE column
  filters.
- Local safety filtering protects correctness when the fetch path cannot be
  narrowed enough.
- Health and support output can report fetch capability and fallback reasons.

Target:

- Every model declares one of these fetch contracts:
  - exact shard-safe NQE filter
  - NQE pushdown parameters
  - hash/bucket partitioning
  - model-scoped fallback with exact local safety filter
  - full fallback with a plain reason

Remaining work:

- Add schema/parity tests proving shard-filtered output equals full-query output
  for the same shard.
- Add hash or bucket contracts for high-volume models without stable natural
  filters.
- Profile real execution metrics before adding query pushdown, then only add
  pushdown where the Forward data model proves identical output shape.
- Persist fetch contract decisions in plan items, model results, execution
  steps, Health, and support bundles.

### 3. Add Faster Apply Engines Without Changing The Workflow

Current state:

- `adapter` is the only active apply engine.
- Capability/fallback reporting exists so operators can see why a model stayed
  on the conservative path.

Target:

- Faster engines such as `bulk_orm`, future TurboBulk, or parquet-backed apply
  are implementation details below Branching and fast bootstrap.
- The operator still selects Branching or fast bootstrap; they do not choose a
  separate "bulk sync" product.

Remaining work:

- Prove `bulk_orm` on the simplest native models first, with equivalent NetBox
  validation, Branching diff visibility, change tracking, statistics, and row
  issue capture.
- Keep complex models on `adapter` until their side effects are proven:
  interfaces, cables, modules, routing, peering, inventory items, and any model
  that creates supporting objects.
- Add engine-specific rollback/failure reporting so a failed fast engine can be
  diagnosed without raw row payloads.
- Keep future TurboBulk/parquet work isolated to the apply-engine boundary.

### 4. Make Fast Bootstrap A First-Class Baseline Tool

Current state:

- Fast bootstrap is explicit and can seed trusted first loads.
- Later Branching diffs still depend on query binding, snapshot lineage, and
  baseline metadata matching the diff contract.

Target:

- Operators can see before starting whether the next run will be true diff,
  reconciliation, or full fallback.
- Fast bootstrap should be a deliberate baseline path, not a hidden shortcut.

Remaining work:

- Expand Health blockers with live-on-demand checks for pinned commit drift,
  latest snapshot lineage, query/library mismatch, and missing baseline
  evidence.
- Persist enough bootstrap evidence for Branching to explain why it can or
  cannot use the bootstrap run as a later diff baseline.
- Document the unavoidable reconciliation cost when switching execution modes,
  and make the UI label that run clearly.

### 5. Make Support And Recovery Fully Self-Service

Current state:

- Health, log export, support bundles, and recovery recommendations cover the
  main local-state questions.
- Execution-run bundles include sanitized linked-ingestion issue summaries and
  runtime bottleneck hints.

Target:

- A customer can export one bundle and support can answer: what happened, what
  failed, which query/map was involved, whether the row was skipped or fatal,
  which branch/job/merge needs action, and what the next safe action is.

Remaining work:

- Add model-specific support-bundle fixtures for routing, virtual chassis,
  IPAM, cabling, modules, and peer/routing plugin objects.
- Add richer bottleneck classification as more timing fields land: Forward
  query, transfer/pagination, row apply, Branching diff, Branching merge,
  PostgreSQL cleanup, and worker timeout.
- Keep exports sanitized by construction: no raw rows, credentials, network
  IDs, snapshot IDs, or customer inventory examples.
- Add a one-button operator workflow to collect support data from sync,
  ingestion, execution run, and failed job surfaces.

### 6. Promote Chaos And Scale Testing Into A Release Gate

Current state:

- `invoke scale-chaos-test` covers deterministic synthetic cases and runs in
  CI.
- Destructive Docker worker-kill testing is still future work.

Target:

- Any change to Branching execution, recovery, shard fetch, apply engines, or
  baseline handling must prove failure recovery before release.

Remaining work:

- Add local Docker failure injection for worker hard-kill before branch
  creation, after branch creation, during row apply, during merge, and during
  finalization.
- Verify support bundles after every injected failure.
- Add simultaneous-worker stress tests for duplicate callbacks and claims.
- Keep Playwright coverage for the native UI surfaces: Health, support export,
  reconcile, retry, requeue merge, discard branch, and execution-run detail.

### 7. Keep Branches And Future NetBox Versions Aligned

Current state:

- `main` carries the stable product workflow.
- NetBox 4.6 compatibility and TurboBulk feature work need ongoing alignment.

Target:

- One shared code branch and supported NetBox versions expose the same
  operator workflow. Differences should be runtime capability only.

Remaining work:

- Backport product-surface changes as capability-gated feature work.
- Keep version-specific NetBox behavior in compatibility layers.
- Keep TurboBulk under apply/fetch engines and feature flags rather than a
  separate product branch.
- Track NetBox native bulk APIs and Branching changes as capability inputs, not
  reasons to create a separate sync path.

### 8. Add Capacity And Database Operations Guidance

Current state:

- Health can report observed shard timing and timeout proximity.
- Branch budgets remain bounded by operational guidance.

Target:

- Operators know before starting whether Branching is appropriate for the
  workload and what database/runtime pressure to expect.

Remaining work:

- Add pre-run timeout-risk estimates using historical execution data.
- Surface branch schema count, merge backlog, stale branch cleanup, and
  PostgreSQL retention/autovacuum guidance.
- Add warnings when planned shard count and observed merge duration make the
  configured job timeout unrealistic.
- Keep fast bootstrap guidance explicit for very large trusted first loads.

### Priority 1: Finish Ledger-First Operations

Status: partially implemented.

The execution ledger is now the right operational boundary for Branching runs,
but old `_branch_run` JSON still exists as compatibility/display state. The
remaining work is to make the ledger the only active source of orchestration
truth.

Work:

- Move every UI summary, API summary, retry decision, merge decision, and
  support-bundle field to ledger-derived state.
- Keep `_branch_run` as read-only upgrade compatibility for one documented
  release window after ledger-derived workflows are proven.
- Make compatibility helpers prefer ledger state when JSON state is absent.
  **Partially done: pending-run and waiting-for-merge checks now fall back to
  execution-run/step state so native UI controls can continue from the ledger.
  Merge eligibility and merge completion now also fall back to the ingestion's
  execution step when `_branch_run` is absent, including final-step baseline
  marking. Active execution-run lookup now falls back to the latest
  non-terminal ledger run, so stage-step claiming can proceed without the JSON
  cache. Late-stage resume now reconstructs persisted plan items and shard
  scope from the active execution ledger when `_branch_run` is absent, while
  completed historical ledger runs are ignored for new planning so they cannot
  suppress preflight. Direct/non-job planned Branching runs now create the
  execution ledger before applying any shard, and fallback plan-item updates no
  longer mutate completed historical ledger runs when no active run exists.
  Sync display parameters, workload summary, execution summary, and activity
  text now derive branch-run presentation from the execution ledger when
  `_branch_run` is absent. Row-apply progress heartbeats now update the active
  execution step as well as compatibility JSON, so ledger-only runs keep
  current row counts, heartbeat timestamps, and shard-aware progress log text.
  Final completion/baseline marking also has a missing-JSON regression.**
- Add a deprecation note and migration plan before removing active writes to
  `_branch_run`. **Done for the architecture plan: `_branch_run` remains a
  compatibility/read-through cache until one release after ledger-derived UI,
  API, recovery, support-bundle, and upgrade behavior pass the scale/chaos gate
  without JSON-only dependencies.**
- Add upgrade tests proving old parameter state can still render and reconcile
  into ledger state.
- Persist run/step evidence needed by support bundles before clearing or
  ignoring compatibility state. The export should remain useful after the final
  shard completes, after branch cleanup, and after a failed reconciliation.
  **Partially done: ledger-derived display/export state now includes explicit
  `state_source=execution_ledger` and `state_synthesized=true` markers when the
  compatibility `_branch_run` cache is absent. Sync and ingestion support
  exports also include `branch_run_state_source` so operators can distinguish
  compatibility cache from reconstructed ledger evidence.**
- Add explicit transition guards around stage, merge, retry, discard, and
  finalize paths so a duplicate job callback cannot silently advance the run
  twice. **Partially done: stage and merge claims are guarded, and retry
  preparation now uses a locked idempotent transition that refuses duplicate
  retry enqueue attempts once the step is queued. Stage claiming now refuses to
  let a different job reclaim a running step, so a duplicate worker cannot
  replace the owning job; the same/no-owner job path can still finish timeout
  bookkeeping. Final completion is also idempotent under a row lock.
  Discard-and-retry now
  locks the step and linked ingestion rows, and a repeated discard cannot
  increment retry count or add a second issue. Broader simultaneous-worker
  stress coverage remains future work.**

Why this matters:

- Long baselines need atomic step ownership and unambiguous recovery.
- JSON read/modify/write state is too easy to race under multiple NetBox
  workers.
- Support should be able to answer "what should the operator do next?" from one
  persisted run record.
- Support should be able to export a complete, sanitized history even after the
  current sync has advanced to a later run.

### Priority 2: Complete Self-Service Diagnostics

Status: implemented for local-state diagnostics, local query-drift
classification, explicit live source reachability export, explicit live
query-drift export, explicit live data-file freshness export, and
ledger-derived capacity projection.

Support bundles now exist at the sync, ingestion, and execution-run level. The
next step is a read-only health/doctor surface that helps operators catch
misconfiguration before starting another multi-day run.

Work:

- Add a sync health/doctor view that reads local plugin state only by default.
  **Done: the sync Health tab renders local-state diagnostics without live
  Forward API calls.**
- Show source status, configured timeout, background job timeout, branch budget,
  execution backend, enabled maps, query binding modes, optional data-file map
  dependencies, latest validation, latest ingestion, latest execution run, and
  current recovery recommendation. **Done for the local-state values.**
- Add an explicit live source reachability check. **Done: the Health tab exposes
  `Export Live Source Check`, which calls Forward only when clicked and exports
  source/network/latestProcessed reachability diagnostics without including the
  configured network ID or snapshot ID in the payload.**
- Show local query-drift diagnostics for enabled maps. **Done: the Health tab
  classifies raw bundled-query matches, locally modified raw queries, repository
  path filename matches/mismatches, and direct query IDs that require live
  Forward lookup for full verification.**
- Add an explicit live query-drift check for repository paths and direct query
  IDs. **Done: the Health tab exposes `Export Live Query Drift Check`, which
  calls Forward only when clicked and exports repository/query-ID source drift
  diagnostics without storing raw query rows.**
- Add explicit data-file freshness diagnostics for optional data-file-backed
  maps. **Done: the Health tab exposes `Export Live Data File Check` when
  enabled maps require known Forward NQE data files. The check runs tiny
  snapshot-scoped NQE probes and exports whether each required data file has
  rows visible in the selected snapshot, without including network IDs or
  snapshot IDs in the payload.**
- Show capacity guidance from execution ledger timing. **Done: the Health tab
  shows completed/remaining steps, average/max completed shard duration, and a
  worker-timeout proximity warning when timing data exists.**
- Warn when the next run is expected to be full/reconciliation instead of true
  diff. **Done for local baseline/query-mode eligibility; exact latest-snapshot
  comparison remains runtime-only.**
- Warn when direct query IDs, repository paths, or commit IDs are missing for
  maps that are expected to use diffs.
- Keep the page read-only on render so opening diagnostics cannot create a new
  Forward/API failure mode. **Done.**

Why this matters:

- Customers should not have to send a sequence of screenshots for each failed
  shard.
- Most current support questions are state questions: query mode, diff
  eligibility, branch state, timeout, stale job, or missing optional map/data
  file.

### Priority 3: Make Fetch Shard-Scoped By Contract

Status: partially implemented.

Current shard filtering is safe where the returned NQE rows expose stable
columns. The final scale target is a model-by-model fetch contract that can
prove whether a shard is device-scoped, IPAM-scoped, hash/bucket scoped, or not
safe to filter.

Work:

- Define a per-model shard fetch contract with:
  - stable shard key family
  - allowed NQE column filters
  - optional query parameters
  - exact local safety filter
  - fallback mode and reason
  **Partially done: Health output now reports the current per-model fetch
  capability and fallback reason for enabled models. The code now also keeps an
  explicit fetch-capability/fallback registry for every supported model, with a
  regression test preventing unsupported implicit defaults.**
- Add hash/bucket contracts for models without one stable natural filter.
- Extend built-in NQE maps with optional query-pushdown parameters when Forward
  can filter before materializing the result set.
- Add schema tests proving shard-filtered output matches full-query output.
- Add support-bundle fields explaining why each step used `shard`, `model`,
  `full_fallback`, or `diff_fallback`.

Why this matters:

- Re-running full model queries for late shards is the main remaining Branching
  performance cost.
- Filtering after fetching is only a correctness guard; it does not solve
  Forward query runtime or transfer volume.

### Priority 4: Add A Conservative Bulk Apply Engine

Status: boundary and capability/fallback reporting are implemented; only
`adapter` is active. The next safe implementation step is proving one model's
faster engine parity, not enabling bulk writes broadly.

The apply-engine boundary exists so faster write engines can be added without
creating another sync workflow. The next safe step is a small `bulk_orm` engine
for simple models only.

Work:

- Add per-model apply-engine capability reporting with the selected engine,
  rejected candidate engines, and a plain-language fallback reason. **Done:
  model results, plan items, sync Health output, and execution-run support
  bundles now expose the selected engine and fallback decision. The apply-engine
  selector now has an explicit classification for every supported model so new
  models cannot silently fall into an unclassified adapter default.**
- Treat `adapter` as the native-safe baseline because it preserves the existing
  per-model validation, coalesce behavior, object saves, event tracking,
  dependency skips, row issue capture, and statistics.
- Implement `bulk_orm` only for simple models with stable identity and no
  model-specific side effects, after tests prove native validation and object
  change tracking remain equivalent to the adapter path.
- Keep complex adapters on `adapter`: interfaces, cables, modules, routing,
  peering, and any model that creates supporting native objects.
- Record engine capability and fallback reason per model in model results,
  execution-step metrics, health output, and support bundles.
- Keep per-row issue capture and counters equivalent to the adapter path.
- Keep Branching review semantics unchanged when a bulk engine runs inside a
  branch.

Why this matters:

- Fast bootstrap and simple Branching shards can get faster without weakening
  the NQE row contract.
- Bulk behavior stays an implementation detail below native NetBox workflows.
- Operators and support should not have to infer whether a model was slow
  because bulk was unavailable, unsafe, or deliberately disabled.

Current decision:

- `bulk_orm` remains deferred until per-model proof exists. A broad ORM bulk
  path would risk bypassing native `save()` behavior, validation hooks, object
  change tracking, Branching diff semantics, and existing row-level issue
  handling. Capability reporting is the correct immediate improvement because
  it makes that decision visible while preserving the proven native path.

### Priority 5: Add Chaos And Scale Release Gates

Status: implemented as a named synthetic/local gate; destructive Docker
hard-kill injection remains future work.

The current CI path proves normal behavior. Scale changes also need local
failure injection to prove recovery.

Work:

- Add a local Docker chaos gate for:
  - stage hard-kill before branch creation
  - stage hard-kill after branch creation
  - stage hard-kill during row application
  - merge hard-kill
  - duplicate stage job
  - duplicate merge job
  - branch-budget overflow
  - late-shard retry with shard-scoped fetch
- Add a concurrency gate for duplicate queue callbacks and simultaneous worker
  claims against the same stage or merge step.
- Export and inspect the support bundle after every forced failure.
- Keep the chaos gate outside fast CI if runtime is too high, but require it
  before releases that change Branching execution, recovery, or ledger state.
- Expand Playwright coverage for reconcile, requeue merge, discard branch and
  retry, export bundle, and the health/doctor view.
- Add a named local synthetic chaos gate. **Done: `invoke scale-chaos-test`
  runs the focused recovery/scale matrix for job timeout, stale reconciliation,
  stale stage-with-branch recovery, stale merge recovery, partial-branch
  discard, duplicate stage/merge callbacks, support-bundle recovery detail,
  ingestion/sync log export, ledger-derived support export after
  compatibility-state removal, and late-shard scope handoff. It is wired into
  `invoke ci` and the release playbook.**

Why this matters:

- The hardest customer failures are worker exits, stale jobs, partial branches,
  and merge timeouts.
- These are not reliably covered by unit tests alone.
- Duplicate callbacks and worker races are correctness issues, not cosmetic log
  issues. The gate should prove that they either no-op or produce one explicit
  recovery recommendation.

### Priority 6: Keep Query Library Drift Visible

Status: implemented for current repository/query/data-file surfaces.

Query IDs and repository paths are the right long-term execution mode for
diffs, but they create a new operator problem: the plugin must explain whether
Forward library state matches the bundled query version.

Work:

- Add query-library drift detection for repository paths and direct query IDs.
  **Done: local query-drift classification and explicit live query-drift export
  are available from the Health tab.**
- Show whether the selected query is behind the bundled map version, missing,
  or pinned to a commit. **Done for the current API surface: local Health
  output shows raw/latest/pinned commit behavior, and explicit live export
  resolves repository paths/direct query IDs, records the requested commit
  revision, and compares returned source to bundled compiled NQE when Forward
  includes source in the response.**
- Add diagnostics for optional data-file-backed maps when the latest snapshot
  has not captured the uploaded data file yet. **Done for known optional data
  files through explicit live data-file freshness export.**
- Keep the bundled raw-query fallback available and documented.
- Keep all examples sanitized and free of customer identifiers.

Why this matters:

- Many observed customer issues were query-library drift, wrong binding mode, or
  stale optional data-file state rather than Python adapter bugs.

### Priority 7: Document And Surface Capacity Planning

Status: partially documented.

The plugin cannot remove NetBox Branching merge cost. It can make expected
runtime and risk visible before the operator starts.

Work:

- Add warnings when planned shard count, observed shard duration, and configured
  background job timeout make completion unlikely.
- Add per-model metrics for query runtime, stage runtime, merge runtime, retry
  count, rows fetched, rows applied, rows skipped, and row failures.
- Add PostgreSQL/Branching retention guidance for large temporary branch schema
  count, cleanup expectations, autovacuum pressure, and disk growth.
- Keep branch budgets bounded by guidance instead of raising them to hide slow
  merges.

Why this matters:

- A 16-hour timeout can still be too short for a massive reviewable Branching
  baseline.
- The correct answer for very large trusted first loads remains fast bootstrap,
  followed by diff-eligible steady-state Branching.

### Priority 8: Keep One Branch And Feature Surfaces Aligned

Status: ongoing.

Keep one shared code branch and one operator workflow across supported NetBox
versions unless a runtime capability blocks it.

Work:

- Keep 4.6-only behavior behind explicit capability checks or optional maps.
- Keep TurboBulk as a feature path behind runtime flags and compatibility
  gates, not a separate branch line.
- Document any feature gap and the NetBox capability that causes it.

Why this matters:

- Customers should not see different sync semantics just because they move
  between supported NetBox runtimes.

## Completion Definition

This scale-hardening effort is complete only when:

- the execution ledger is the active source for Branching orchestration and
  recovery
- the sync health/doctor surface answers the common support questions without
  live API side effects
- shard-scoped fetch contracts exist for every model, including explicit
  fallback reasons
- at least one conservative faster apply engine is proven or explicitly
  deferred with documented blockers
- query-library drift and optional data-file freshness are visible before sync
  start
- local chaos/recovery gates cover stale jobs, partial branches, merge failures,
  duplicate callbacks, and late-shard retry
- support bundles contain enough sanitized detail to troubleshoot merge,
  routing, virtual-chassis, IPAM, and row-level failures without screenshots
- support bundles remain useful after completion, failure cleanup, branch
  cleanup, and compatibility `_branch_run` state removal
- stage, merge, retry, discard, and finalize transitions have explicit
  idempotency/concurrency coverage
- `invoke ci` and the scale/chaos release gate both pass

## Completion Audit

Last audited: 2026-05-16.

This audit maps the completion definition to current evidence. Passing CI is
required, but it is not sufficient by itself; every architectural requirement
below must have direct implementation and test evidence before this plan can
move to completed.

| Requirement | Current evidence | Status |
| --- | --- | --- |
| Execution ledger is the active source for Branching orchestration and recovery. | Ledger run/step models, stage/merge/retry/discard/finalize helpers, old `_branch_run` upgrade, missing-JSON display/retry/merge coverage, ledger-derived support exports, direct planned runs creating ledger state before shard apply, completed historical ledgers ignored by new planning/fallback updates, and public pending/merge gates that ignore stale compatibility JSON once a real execution run exists. Remaining `_branch_run` writes are retained as a documented one-release compatibility/read-through cache and are not the primary control plane when a ledger run exists. | Covered for current tranche; removal deferred |
| Sync health/doctor answers common support questions without live API side effects. | Health tab renders local diagnostics without Forward API calls and exposes explicit live source, query-drift, and data-file exports only when clicked. Playwright and Django tests cover the surface. | Covered |
| Shard-scoped fetch contracts exist for every model, including fallback reasons. | Health output and fetch registry report fetch mode, key family, schema contract, local safety-filter guarantee, and fallback reason for every supported model; device-scoped and selected IPAM filters exist. Shard-parameter built-in NQE maps now guard primary device iterators with `forward_netbox_shard_keys` while preserving global peer inference. Ledger-only late-shard resume has executor-level coverage proving persisted shard scope reaches native NQE filters without `_branch_run`. Hash/bucket contracts, live schema-parity fixtures, and measured live query-pushdown proof remain future work that requires live Forward runtime evidence and a proven deterministic bucket primitive. | Covered for current tranche; live proof deferred |
| At least one conservative faster apply engine is proven or explicitly deferred with documented blockers. | Apply-engine boundary records `adapter`; every model is classified as future candidate or adapter-required, and broad `bulk_orm` is explicitly deferred until native parity proof exists. | Covered as deferred |
| Query-library drift and optional data-file freshness are visible before sync start. | Local query-drift classification, pinned-versus-latest commit guidance, structured next-run diff blockers, and explicit live query/data-file exports are present in Health and support-bundle surfaces. Live query export records requested commit revision and compares returned source to bundled compiled NQE when Forward includes source. | Covered |
| Local chaos/recovery gates cover stale jobs, partial branches, merge failures, duplicate callbacks, and late-shard retry. | `invoke scale-chaos-test` covers deterministic stale/retry/merge/duplicate/late-shard/log-export scenarios. Synthetic stale-worker coverage now includes before-branch, after-branch/row-apply, and merge phases with support-bundle recovery recommendations. Destructive Docker worker-kill cases remain intentionally deferred because they require a separate local failure-injection harness and should not be a default CI side effect. | Covered for current tranche; destructive gate deferred |
| Support bundles contain enough sanitized detail for merge, routing, virtual-chassis, IPAM, and row-level failures without screenshots. | Sync, ingestion, and execution-run support bundles include run/step/job/log/model/result details. Execution-run step entries include linked-ingestion issue counts and sanitized issue samples without raw row payloads. Synthetic bundle fixtures cover representative cabling, module, virtual-chassis, IPAM, and routing issue summaries. | Covered |
| Support bundles remain useful after completion, failure cleanup, branch cleanup, later runs, and compatibility `_branch_run` removal. | Execution-run bundle tests cover branch cleanup, old run evidence after a later run starts, failed-run recovery evidence after compatibility state is cleared, and sync/ingestion exports that label ledger-derived state when `_branch_run` is absent. | Covered |
| Stage, merge, retry, discard, and finalize transitions have explicit idempotency/concurrency coverage. | Targeted row-lock/idempotency tests cover stage, duplicate running-stage claim refusal, merge, retry, discard, and finalize. Transaction-backed thread tests now prove simultaneous stage, merge, retry, discard, and finalize attempts resolve to one owner/effective transition. | Covered |
| `invoke ci` and the scale/chaos release gate pass. | `invoke ci` passed on 2026-05-16 after the ledger-only late-shard resume, direct planned-run ledger persistence, historical-ledger mutation guard, and merge-timeout ordering updates. The full gate included harness checks, sensitive-content scanning, pre-commit, Docker build/start, Django checks, the 92-test `invoke scale-chaos-test` gate, the 424-test Django suite, Playwright UI harness, docs, and package build. | Covered |

Current conclusion: the current implementation is complete for this
scale-hardening tranche and release-gated. The remaining long-term items are
explicitly deferred with blockers/evidence above and should be tracked as future
architecture work, not as unresolved current-tranche blockers.

Follow-on execution is tracked in:
`docs/03_Plans/active/2026-05-16-deferred-risk-tranche.md`.

## Next Tasks

Track the remaining architecture work as a small execution list:

- [x] Prove the one-release `_branch_run` retirement window across upgrade,
  cleanup, later-run, and support-bundle export paths for current release
  scope. Current status: ledger-first behavior is covered; total removal of
  active compatibility writes is explicitly deferred until one release of
  ledger-derived UI, API, recovery, support-bundle, and upgrade behavior has
  passed the release gate.
- [x] Expand shard-fetch parity with live schema fixtures for the supported
  models that still only have synthetic parity coverage. Current status:
  synthetic schema/contract coverage and live export hooks are in place; live
  fixture capture is explicitly deferred because it requires external Forward
  runtime data and must not commit customer identifiers or rows.
- [x] Validate the `forward_netbox_shard_keys` query-side pushdown path against
  live fixtures and measured runtime/row-volume evidence, then document the
  deterministic bucket primitive that would unlock non-device hash/bucket
  pushdown. Current status: built-in query guards and local regressions are in
  place; measured live pushdown and hash/bucket primitives are explicitly
  deferred until Forward-side runtime evidence proves the shape and benefit.
- [x] Prove or explicitly defer a conservative faster apply engine for a small
  simple-model set. Current status: explicitly deferred with per-model
  capability evidence until native validation/change-tracking parity is proven.
- [x] Add true Docker worker-kill chaos coverage before branch creation, after
  branch creation, during apply, and during merge. Current status:
  deterministic stale-worker simulations cover those recovery decisions in
  `invoke scale-chaos-test`; destructive Docker process-kill injection is
  explicitly deferred to a separate opt-in harness because it mutates local
  worker containers and should not run as part of ordinary CI.
- [x] Keep the single shared code branch aligned across 4.5 and 4.6 with
  capability-gated 4.6-only surfaces and no forked product semantics.
- [x] Keep TurboBulk as feature work under flags and compatibility gates until
  a proven faster apply path justifies promoting it further.
- [x] Keep support bundles actionable after cleanup and later runs, and make
  sure every recovery recommendation is rendered without screenshots or raw
  row data.

## Self-Service Completion Checklist

Before calling the architecture self-service for large customer deployments,
the plugin should let an operator answer these questions from native NetBox
surfaces:

- Is my source reachable, configured with sane timeouts, and using the expected
  backend?
- Are enabled maps bound to raw query text, repository paths, or direct query
  IDs, and are those bindings eligible for diffs?
- Is a query using the bundled contract, locally modified text, a known
  repository path, or an unverified direct query ID?
- Has every optional data-file-backed map been captured by a processed Forward
  snapshot?
- Will the next run be a true diff, reconciliation run, or full fallback, and
  why?
- Which shard is currently running, which shards are retryable, and which
  branch or merge job needs operator action?
- Which model or phase is the runtime bottleneck: Forward query, row apply,
  Branching diff, Branching merge, or PostgreSQL cleanup?
- Can support diagnose the failure from one sanitized export without raw
  customer rows, screenshots, network IDs, snapshot IDs, or credentials?

## Detailed Target Notes

The sections below preserve the implementation notes and acceptance criteria for
each target. They are intentionally more detailed than the priority backlog
above so future work can move directly from this plan into scoped
implementation tranches.

### 1. Execution Ledger As The Operational Source Of Truth

The execution ledger now exists, but the old `_branch_run` JSON state is still a
compatibility/display cache. The long-term target is:

- new Branching orchestration writes the ledger first
- UI summaries, API summaries, recovery actions, and support bundles derive
  from the ledger
- `_branch_run` remains readable only for upgrade compatibility and then becomes
  removable after a documented deprecation window
- every state transition records enough context to explain why the next action
  is retry, requeue merge, discard branch, wait for review, or complete

#### `_branch_run` Deprecation Plan

`_branch_run` is no longer the target source of truth. It remains a
compatibility/read-through cache for old runs, in-flight upgrades, and existing
NetBox UI flows that still expect sync-parameter state.

Removal should happen in stages:

1. **Current release window:** write the execution ledger first; write
   `_branch_run` only as a compatibility cache for views, existing job entry
   points, and upgrade-safe resume.
2. **One full release after ledger parity:** keep reading `_branch_run`, but
   treat missing JSON as normal. All stage, merge, retry, discard, finalize,
   support-bundle, health, and API paths must derive their active decisions from
   `ForwardExecutionRun` and `ForwardExecutionStep`.
3. **Removal candidate:** stop active writes to `_branch_run` only after upgrade
   tests prove old sync parameters can be rendered, reconciled into ledger
   state, or safely marked non-resumable with a clear operator recommendation.
4. **Final cleanup:** remove compatibility writes and retain a narrow reader
   only if supported upgrade paths still need it.

Removal is blocked until these checks exist:

- a fixture or migration-style test for an old `_branch_run` payload
  **Done: old JSON-only plan-item payloads without `execution_run_id` are
  upgraded into `ForwardExecutionRun`/`ForwardExecutionStep` records and
  resolved through the ledger without writing `execution_run_id` back into the
  compatibility cache.**
- a missing-JSON recovery test for stage claim, merge eligibility, merge
  completion, next-stage queueing, retry, discard, finalize, and support export
  **Covered so far for pending/waiting state helpers, active run lookup, stage
  claim, merge eligibility, merge completion, next-stage queueing, finalize,
  retry, discard, and support export after cleanup. Old-payload upgrade
  reconciliation is now covered.**
- a support-bundle test proving completed or cleaned-up runs remain
  diagnosable without `_branch_run`
- a Playwright or API check proving native operator actions still render from
  ledger state
- `invoke scale-chaos-test` and `invoke ci` pass with the compatibility state
  absent in representative recovery scenarios

Remaining work:

- Add reconciliation event history so support bundles show what the recovery
  logic observed and changed, not only the final step state. **Done: execution
  runs retain bounded reconciliation events and expose them through API/support
  summaries.**
- Add applied row counters per execution step so fetched rows, attempted rows,
  applied rows, skipped rows, and failed rows are all visible in one place.
  **Done for Branching execution steps; counters are derived from existing
  per-model row statistics and included in API/table/support-bundle output.**
- Add a release migration plan for eventually retiring `_branch_run` as an
  active orchestration source.
- Add an immutable evidence policy for what must remain on the run/step records
  after branch cleanup and after sync compatibility state is cleared.
  **Partially done: support-bundle tests now prove merged step evidence keeps
  the branch name and ingestion link after the native Branch object is cleaned
  up and `_branch_run` is cleared.**
- Add optimistic transition checks or equivalent row-lock semantics to the
  remaining orchestration paths that still depend on mutable sync-level state.
  **Partially done: merge jobs now claim the linked execution step under a row
  lock and skip duplicate workers when another unfinished merge job already owns
  the step.**
- Keep merge eligibility/completion ledger-first. **Partially done: mergeable
  ingestions can be identified from staged execution steps without `_branch_run`,
  and merge completion updates the linked execution step/run directly. Next
  stage queueing can rebuild the minimal compatibility state from the execution
  run when `_branch_run` is absent, then reuse the existing native stage-job
  enqueue path. Stage workers can also reconstruct that minimal state from the
  latest non-terminal execution run before claiming the next step. Sync enqueue
  continuation and stage-job handoff are now ledger-only once a run exists,
  including reconstruction of the persisted plan item and shard scope from
  `ForwardExecutionStep` when `_branch_run` is absent.**

### 2. Shard-Scoped Fetch Everywhere It Is Safe

Shard-scoped fetch is the best Branching-specific performance target because it
reduces repeated Forward query work without changing NetBox review semantics.
The current implementation persists fetch scope, uses native NQE column filters
for safe device-scoped and selected IPAM cases, and guards primary device loops
inside shard-parameter built-in NQE maps.

Remaining work:

- Add hash/bucket shard contracts for models that cannot be filtered by one
  stable natural column.
- Add deeper built-in NQE query-pushdown parameters where the Forward data model
  can filter before result materialization, not only during result pagination.
  **Partially done: shard-parameter built-in maps now receive
  `forward_netbox_shard_keys` and guard primary `device` iterators. Query
  registry tests prove those guards exist and that `peer_device` inference
  lookups stay global for routing correctness. Live performance profiling and
  hash/bucket contracts remain open.**
- Record query runtime and fetched-row counts for every step from real fetch
  telemetry, including fallback cases.
- Make the support bundle explicitly explain why a step used `shard`,
  `model`, `full_fallback`, or `diff_fallback`. **Done: execution-run support
  bundles now include a plain-language `fetch_explanation` for each step and
  per-step metrics entry.**
- Make the Health tab summarize which enabled models have shard-safe native
  column filters and which still use model-fetch fallback. **Done: Health now
  includes fetch contract modes, shard-safe counts, fallback counts, and
  per-model reasons. Every supported model has an explicit capability or
  fallback entry in the shard-fetch registry.**
- Add tests proving late-shard retry does not refetch unrelated rows when a
  shard-safe contract exists. **Done: an executor-level ledger-only resume
  regression proves a persisted `dcim.interface` shard scope is reconstructed
  from `ForwardExecutionStep`, passed into native NQE column filters, locally
  safety-filtered, and retained on the staged plan item without writing
  `_branch_run`.**

### 3. Bulk Apply As An Engine, Not A Workflow

The apply-engine boundary is in place and records `adapter`. That is the right
long-term shape: performance engines sit below Branching and fast bootstrap,
while the user still chooses the native execution backend.

Remaining work:

- Add model-level capability reporting before enabling any faster engine so
  support bundles and Health output can explain why `adapter` was selected.
  **Done: the selected engine, rejected candidates, reason code, and
  plain-language reason are present in model-result metadata, plan snapshots,
  sync Health output, and execution-run support bundles. A regression test
  requires every supported model to be classified as either a future
  `bulk_orm` candidate or adapter-required with an explicit blocker.**
- Implement a conservative `bulk_orm` engine only for simple models whose
  identity, validation, side effects, and change tracking are fully understood.
- Keep complex models on `adapter` until proven: interfaces, cables, modules,
  routing plugin models, and anything that creates supporting objects.
- Keep TurboBulk/parquet work on the experimental branch until the NetBox
  runtime exposes a stable supported surface.
- Add per-model capability reporting so an operator can see why a model used
  `adapter` rather than a faster engine.

Non-goal:

- Do not create a separate "bulk sync" product flow. Branching and fast
  bootstrap remain the execution lanes; bulk is an implementation detail under
  those lanes.

### 4. Fast Bootstrap To Branching Continuity

Fast bootstrap solves the trusted first-load problem, but the first later
Branching run can still do reconciliation work if the direct-write baseline and
Branching/diff metadata are not perfectly aligned.

Remaining work:

- Make the UI explain whether the next Branching run is expected to be a true
  Forward diff, a reconciliation run, or a full fallback.
- Record baseline eligibility per map, including query mode, query ID/path,
  commit ID, snapshot ID, and dirty-run blockers.
- Add a run diagnostic that answers "why did this run full-sync instead of
  diff?" without reading logs. **Partially done: Health `next_run` now includes
  structured blockers for fixed snapshot selectors, missing baseline-ready
  ingestions, and raw-query maps that cannot use Forward diffs. The Health UI
  renders those blocker messages before sync start.**
- Keep preventing dirty or row-failed runs from becoming diff baselines.

### 5. Recovery And Chaos Testing As A Gate

Unit tests are not enough for multi-day Branching runs. Recovery behavior needs
forced failure tests that prove the ledger and native branches stay coherent.

Remaining work:

- Add a local Docker chaos gate for stage hard-kill before branch creation,
  stage hard-kill after branch creation, hard kill during row application, merge
  hard-kill, duplicate stage job, duplicate merge job, branch-budget overflow,
  and late-shard retry. **Partially covered by deterministic stale-worker
  synthetic tests for the recovery decisions; true container/process
  interruption remains future work.**
- Export and inspect the run support bundle after each forced failure.
- Keep these tests out of the default fast CI path if runtime is too high, but
  require them before release trains that change Branching execution.
- Add Playwright coverage for the recovery surfaces: execution run, execution
  step, reconcile, requeue merge, discard branch and retry, and export bundle.

### 6. Self-Service Operator Experience

The goal is that a customer can diagnose most sync problems without sending
screenshots one at a time.

Remaining work:

- Add a one-button support bundle on every relevant surface: sync, ingestion,
  execution run, and failed job context. **Done for sync, ingestion, and
  execution-run surfaces; failed job context is covered through linked job
  details in the bundle. Execution-run step entries include linked-ingestion
  counters and sanitized issue samples, excluding raw row/default payloads.**
- Add a health/doctor view for source reachability, Forward query mode,
  repository query binding, data-file freshness, plugin versions, Branching
  availability, worker timeout, and branch-budget settings. **Partially done:
  the sync Health tab shows local source status, query mode, data-file map
  hints, plugin/runtime settings, Branching availability, timeout settings,
  branch budget, latest validation/ingestion/execution run, and the current
  recovery recommendation, local query-drift classification without making live
  Forward API calls, an explicit live source reachability export, and an
  explicit live query-drift export for repository paths/direct query IDs, and an
  explicit live data-file freshness export for known optional data-file-backed
  maps.**
- Add "current recommendation" text derived from the ledger: wait, merge,
  requeue merge, retry, discard branch and retry, rerun validation, or contact
  support with the exported bundle. **Done for execution runs: support bundles
  and the native detail view expose the current ledger-derived recommendation.**
- Add clearer progress around preflight versus query fetch versus row apply
  versus branch merge so long quiet periods do not look stalled.

### 7. Query Contract And Library Hygiene

NQE remains the normalization/source-of-truth layer. Python should not fix
source data by inventing hidden transforms.

Remaining work:

- Keep built-in query contract tests paired with every query update.
- Add schema checks for shard-filtered query variants against the full-query
  schema.
- Add query-library drift detection so operators can see whether their
  repository query path or direct query ID is behind the bundled version.
  **Done for the current API surface: local query-drift classification now
  flags raw query edits, repository path filename mismatches, bundled filename
  matches, direct query IDs that require live Forward lookup for full
  verification, and raw/latest/pinned commit behavior. The Health tab also has
  an explicit live export that resolves repository paths/direct query IDs,
  records the requested commit revision, and compares returned source to bundled
  compiled NQE when Forward includes source in the response.**
- Keep data-file-dependent queries paired with diagnostics that explain when a
  snapshot has not yet captured the uploaded data file. **Done for the known
  optional `netbox_device_type_aliases` and `netbox_feature_tag_rules` data
  files through the explicit Health tab live data-file export.**
- Keep all query examples and tests sanitized of customer identifiers, network
  IDs, snapshot IDs, and private inventory rows.

### 8. Operational Sizing And Database Guidance

Large Branching runs are bounded by NetBox worker timeout, PostgreSQL throughput,
Branching diff/merge cost, and Forward query runtime. The plugin should surface
what it can detect and document the rest.

Remaining work:

- Document recommended worker timeout, worker count, database sizing, and
  branch retention expectations for large baselines. **Partially done:
  timeout, worker capacity, database capacity, and branch-budget guidance are
  documented in the user guide; deeper PostgreSQL maintenance guidance remains
  future work.**
- Add warnings when the planned branch count and observed shard duration make a
  run unlikely to finish within the configured background job timeout.
  **Partially done: the Health tab reports ledger-derived shard timing and warns
  when observed shard duration approaches the configured worker timeout.**
- Add metrics for query runtime, stage runtime, merge runtime, and retry count
  by model so slow phases are visible. **Partially done: execution-run support
  bundle metrics include query runtime, stage duration, merge duration, retry
  counts, and a `bottleneck` field naming the largest measured phase among
  Forward query, row apply/stage overhead, and Branching merge.**
- Keep branch budgets bounded by operator guidance instead of raising them to
  hide slow runs.

### 9. Branch Alignment

Keep one shared code branch and one product surface across supported NetBox
versions unless a runtime capability is missing.

Remaining work:

- Keep 4.6-only behavior behind explicit capability checks or optional maps.
- Keep TurboBulk as a feature path behind runtime flags.
- Document any feature that cannot be carried across versions and the NetBox
  capability that blocks it.

### 10. Long-Term Performance Targets

After the current ledger and health work, the two highest-value performance
targets are:

1. **True shard-scoped NQE fetch.** Use per-model contracts to avoid repeated
   full-model NQE execution during Branching retries and late shards. The ideal
   version filters inside the NQE query or with stable query parameters before
   large result sets are materialized. Native result column filters remain a
   safe intermediate step, but they are not the final answer for very large
   models.
2. **Capability-gated apply engines.** Keep the current adapter engine as the
   correctness baseline, then add faster engines only where NetBox-native
   validation, change logging, Branching semantics, and row-level issue capture
   can be proven equivalent.

These two targets should not weaken the core contract: NQE shapes the data,
NetBox native models apply the data, and Branching remains the review path when
the operator selects it.

### 11. Self-Service Support Targets

The operator experience should eventually support this loop without developer
assistance:

1. Open Sync Health and verify source reachability, query binding drift,
   optional data-file freshness, Branching availability, timeout posture, and
   next-run diff eligibility.
2. Start the selected backend with a clear expectation: fast bootstrap,
   true-diff Branching, reconciliation Branching, or full fallback.
3. Watch progress by phase: snapshot/query resolution, preflight, fetch, row
   apply, branch staging, merge, cleanup, and baseline marking.
4. If a run stalls or fails, use the ledger recommendation to retry, requeue
   merge, discard/retry, wait for review, or export a support bundle.
5. Send one sanitized support bundle that includes the run, steps, jobs, logs,
   model results, issue summaries, query references, and recovery history
   without raw customer rows or private identifiers.

## Constraints

- Keep NQE as the only normalization and model-shaping layer.
- Keep Branching runs native: one reviewable native branch per shard, native
  merge, native branch cleanup.
- Keep fast bootstrap explicit because it does not provide Branching diff
  review.
- Do not introduce hidden Python-side transforms or source-specific shortcuts.
- Do not persist raw customer rows in execution state, logs, tests, docs, or
  support bundles.
- Keep old `_branch_run` state readable until the ledger-backed workflow is
  proven across upgrades.

## Touched Surfaces

Expected implementation surfaces across the remaining work:

- `forward_netbox/models.py`
- `forward_netbox/migrations/`
- `forward_netbox/jobs.py`
- `forward_netbox/views.py`
- `forward_netbox/forms.py`
- `forward_netbox/tables.py`
- `forward_netbox/api/`
- `forward_netbox/utilities/execution_ledger.py`
- `forward_netbox/utilities/query_fetch.py`
- `forward_netbox/utilities/multi_branch_planner.py`
- `forward_netbox/utilities/multi_branch_executor.py`
- `forward_netbox/utilities/multi_branch_lifecycle.py`
- `forward_netbox/utilities/fast_bootstrap_executor.py`
- `forward_netbox/utilities/sync.py`
- `forward_netbox/utilities/sync_primitives.py`
- `forward_netbox/queries/`
- `forward_netbox/tests/`
- `scripts/playwright_forward_ui.mjs`
- `docs/00_Project_Knowledge/`
- `docs/01_User_Guide/`
- `docs/02_Reference/`

## Approach

The remaining work is split into independent workstreams so we can land
recovery/observability safely before changing fetch or apply mechanics.

### Workstream 1: Shard-Scoped NQE Fetch

#### Outcome

A retry or late-stage shard should fetch only the rows needed for that shard
when the model can be deterministically filtered in NQE. The support bundle
should state whether the step used shard-scoped fetch or a fallback.

#### Design

Persist a shard fetch contract on each `ForwardExecutionStep`:

- `fetch_mode`: `shard`, `model`, `full_fallback`, or `diff_fallback`
- shard key family: device, site, prefix, VLAN, VRF, model identity, or hash
  bucket
- shard keys or bucket range
- row-count estimate used for planning

Extend the planner so each `BranchPlanItem` can expose a stable `fetch_scope`.
The execution ledger should persist that fetch scope when it creates the step.

Extend query execution so built-in maps can accept optional parameters such as:

- `forward_netbox_shard_mode`
- `forward_netbox_shard_keys`
- `forward_netbox_shard_bucket`
- `forward_netbox_shard_bucket_count`

The exact parameter names can change during implementation, but the rule is
fixed: an unfiltered query and a shard-filtered query must return the same row
shape for the same model.

#### Model Priority

Start with models where shard identity already matches existing planning:

- `dcim.device`
- `dcim.interface`
- `dcim.cable`
- `dcim.inventoryitem`
- `dcim.module`
- `extras.taggeditem`
- routing and peering models when their device identity is clear

Then evaluate IPAM models:

- `ipam.ipaddress`
- `ipam.prefix`
- `ipam.vlan`
- `ipam.vrf`

IPAM may need hash buckets or prefix/VRF grouping instead of device keys.
Current implementation uses native column filters only where safe:

- `ipam.ipaddress`: device-scoped when the row exposes `device`.
- `ipam.prefix`: broad `prefix` filter, then exact local shard filtering for
  `prefix`/`vrf` membership.
- `ipam.vlan`: broad `vid` filter, then exact local shard filtering for
  `site`/`vid` membership.
- `ipam.vrf`: broad `rd` or `name` filter when every key exposes the same field.

#### Tests

- Planner persists fetch scope on execution steps. **Done for execution ledger
  fields and plan-item snapshots.**
- A staged retry for a shard-capable model calls NQE with shard parameters.
  **Partially done: single-device shards use NQE column filters; multi-device
  device-scoped shards now use native NQE `EQUALS_ANY` column filters. The
  `forward_netbox_shard_*` parameter contract remains persisted for future
  query-pushdown-capable maps.**
- A staged retry for a non-shard-capable model records model/full fallback.
- Shard-filtered built-in queries pass the same schema contract as the full
  query.
- Diff runs preserve the baseline snapshot and use shard filtering where
  possible. **Done for device-scoped `nqe_column_filter` shards, including
  multi-device `EQUALS_ANY` filters.**
- Log export records `fetch_mode` for every step.
- Ledger-derived branch-plan exports are deterministic and provenance-labeled.
  **Done: `branch_run_state_from_execution_run()` orders steps by
  `index/kind/pk` and marks synthesized state; sync-state and log-export tests
  cover ordering and source labels.**

### Workstream 2: Bulk Apply Engine

#### Outcome

Fast bootstrap and eligible future execution steps should be able to use a
high-throughput apply engine while preserving the same validation, issue, and
change-tracking contract.

#### Design

Create an apply-engine abstraction below the existing execution backends:

- `adapter`: current row-by-row NetBox ORM behavior
- `bulk_orm`: batched native ORM behavior where model semantics are simple
- `turbobulk`: future NetBox Labs TurboBulk path when available
- `parquet_bulk`: future parquet-native path for TurboBulk-capable deployments

The execution backend still decides review semantics:

- Branching backend creates branches and may use an apply engine inside a branch
  when safe.
- Fast bootstrap backend writes directly and may use an apply engine for large
  trusted baselines.

The apply engine must report:

- created, updated, deleted, skipped, failed
- per-row issues when row-specific failures occur
- model-level fallback reason when the engine cannot safely handle a model
- native NetBox object changes for direct-write paths

Current state: the abstraction is in place and `bulk_orm` is enabled behind an
explicit per-sync feature flag for a narrow proven safe set. Branching, fast
bootstrap, and legacy sync execution call through this boundary, execution
metadata records selected apply engine per step, and support surfaces expose why
models stayed on the adapter path. `turbobulk` and `parquet_bulk` names remain
reserved and do not change runtime behavior on this branch.

#### Model Priority

Current proven safe set (`bulk_orm` opt-in):

- `dcim.site`
- `dcim.manufacturer`
- `dcim.devicetype`
- `ipam.vrf`
- `ipam.vlan`

Keep these on the adapter path until proven:

- `dcim.devicerole` (`tree_model_constraints`: nested-set fields are not
  preserved by current bulk upsert parity)
- `dcim.platform` (`tree_model_constraints`)

- `dcim.cable`
- `dcim.interface`
- `dcim.module`
- routing plugin models
- anything that creates supporting native objects as a side effect

#### Tests

- Engine selection is recorded on the execution step or ingestion model result.
  **Done for the adapter engine.**
- Unsupported models fall back to adapter behavior. **Done by construction: the
  selector currently returns adapter for every model/backend.**
- Counters match current adapter behavior.
- Direct-write object changes still appear for fast bootstrap.
- Branching branch diffs still appear when bulk execution is used inside a
  branch.
- Per-row failures remain visible and do not abort unrelated models.

### Workstream 3: Recovery And Reconciliation

#### Outcome

Operators should not need to infer recovery from raw job state. They should have
native actions that reconcile a run and tell them exactly which step can be
retried, merged, discarded, or left for review.

#### Actions

Add native UI/API actions:

- `Reconcile Run`
- `Retry Current Step`
- `Requeue Merge`
- `Discard Failed Branch And Retry` **Done: normal retry refuses failed steps
  with an unmerged partial branch; the explicit discard action detaches the
  ingestion, deletes the Branching branch, records an ingestion issue, and
  queues a retry.**
- `Export Run Bundle`

#### Reconciliation Rules

For each ledger step, compare:

- step status
- stage job status
- merge job status
- branch status
- ingestion status and issues
- heartbeat age

Recommended state transitions:

- running step + dead/stale job + no branch: mark retryable
- running step + branch with unmerged changes: require merge requeue or discard
- staged step + missing merge job + auto-merge enabled: offer requeue merge
- merge timeout + branch still present: offer requeue merge
- terminal step + duplicate job callback: ignore duplicate callback

#### Tests

- A hard-killed stage job becomes retryable after reconcile. **Partially done:
  timed-out/failed stage jobs are ledger-visible and retryable; a stale
  running stage with no recorded branch is now marked failed/retryable during
  reconcile. Hard-kill cases after branch creation still need explicit discard
  semantics before automatic retry.**
- A staged branch with no merge job offers requeue merge. **Partially done:
  requeue-merge action is exposed and guarded by `can_queue_merge()`.**
- A partial branch requires explicit discard before retry. **Done: retry-current
  ignores failed steps with attached branches, and discard-branch-retry is a
  separate explicit UI/API action.**
- Duplicate callbacks do not enqueue extra shards. **Done.**
- Duplicate stage workers cannot reclaim a running step or replace the recorded
  owner job, while the same/no-owner running step can still finish failure and
  timeout bookkeeping. **Done.**
- Reconcile output appears in the support bundle. **Done: support bundle
  exposes reconciled run/step state and bounded reconciliation event history.**
- Support bundle export still contains actionable run/step/job/branch evidence
  after the run has completed and compatibility state has been cleared.
  **Partially covered: sync and ingestion support bundle tests now assert that
  ledger-derived branch-run state is exported with explicit provenance when
  `_branch_run` is absent. Execution-run bundle tests also prove that old run
  evidence remains actionable after branch cleanup and after a later run starts
  on the same sync.**
- Simultaneous stage/merge/retry/discard/finalize claims cannot advance the same
  step more than once. **Done: transaction-backed concurrency tests prove two
  workers racing for the same transition produce one owner/effective state
  change.**

### Workstream 4: Run-Level UI And API Surfaces

#### Outcome

The execution ledger should be visible without reading JSON exports.

#### UI

Add native NetBox views:

- execution run list, scoped from a sync **Done**
- execution run detail **Done**
- execution step table **Done**
- support bundle download from sync, run, and ingestion surfaces **Done: sync
  detail exports a sync-scoped support bundle that includes latest ingestion and
  execution-run details.**

Show:

- current phase and heartbeat
- current step
- completed/failed/retryable step counts
- linked ingestion, branch, stage job, and merge job
- fetch mode and apply engine per step
- recovery recommendation **Done on execution-run detail.**

#### API

Add read-only endpoints:

- `/api/plugins/forward/execution-run/` **Done**
- `/api/plugins/forward/execution-step/` **Done**

Add action endpoints where permissions allow:

- reconcile **Done**
- retry current step **Done**
- export bundle **Done**

#### Tests

- API serializers include sanitized run/step data.
- UI list/detail views render seeded execution runs.
- Playwright verifies execution visibility and no mobile overflow.
- Permissions follow normal NetBox model/action patterns.

### Workstream 5: Query Contract And Schema Gates

#### Outcome

Shard-scoped query changes must not drift from the full-query contract.

#### Tests

For every built-in map:

- required fields are present
- coalesce fields are present
- slug-like fields remain valid
- shard-filtered mode returns the same schema as full mode
- query-ID/repository-path mode and raw-query mode expose the same model fields
- optional data-file queries still fail safely when the snapshot does not expose
  the data file

#### Documentation

Update:

- built-in NQE map reference
- model mapping matrix
- troubleshooting guide
- initial baseline strategy
- release validation matrix

### Workstream 6: Scale And Chaos Validation

#### Outcome

Large-sync changes should have a repeatable local gate beyond unit tests.

#### Scenarios

Create local Docker scenarios for:

- stage timeout
- stage hard kill before branch creation
- stage hard kill after branch creation
- stage hard kill during row application
- merge timeout **Covered by synthetic reconcile/support-bundle scenario.**
- merge hard kill
- duplicate stage job **Covered by synthetic terminal-step claim scenario.**
- duplicate merge job **Covered: merge jobs now skip idempotently when the
  ingestion branch is already merged or cleaned up.**
- branch-budget overflow and split
- late-shard retry using shard-scoped fetch
  **Covered: run-next-shard execution passes persisted model and shard scope
  back into planning, and fetcher tests cover native column filters/diff filters
  for shard-safe models.**
- support bundle export after every failure case **Partially covered for merge
  timeout, terminal duplicate-stage scenarios, and explicit ingestion/sync
  support export with ledger-derived branch state; broader destructive
  forced-failure export matrix remains pending.**

#### Local Gate

Run the focused synthetic scale/recovery gate with:

```bash
invoke scale-chaos-test
```

This gate is intentionally deterministic and runs inside the local NetBox Docker
test environment. It complements, but does not replace, future destructive
worker-kill testing.

#### Metrics To Capture

- total runtime
- Forward query runtime per step
- rows fetched per step
- rows applied per step
- branch changes per step
- merge duration per step
- retry count
- fetch mode
- apply engine

Current support bundles now include a metrics section with persisted step count,
estimated/actual changes, fetched row counts, query runtime, attempted/applied/
skipped/failed row counts, retry totals, fetch modes, apply engines, per-step
stage duration, and per-step merge-job duration.

### Workstream 7: Future TurboBulk Feature Work

#### Outcome

Keep the shared branch clean while TurboBulk feature paths mature.

#### Branch Strategy

- Shared branch: stable Branching + fast bootstrap + ledger.
- `turbobulk`: experimental TurboBulk/parquet apply engine.
- `4.6`: NetBox 4.6 compatibility work, including any native bulk APIs or new
  models that can be supported safely.

Feature surface should stay aligned across branches unless a branch lacks the
required NetBox capability. If a feature cannot be supported on one branch,
document the reason in that branch's architecture notes.

## Suggested Order

1. Add run/step UI/API visibility and full run-level export. **Done.**
2. Add stale-job reconciliation and retry actions. **Partially done; destructive
   branch discard is now implemented as an explicit action.**
3. Add shard fetch scope to the planner and execution ledger. **Done.**
4. Implement shard-scoped NQE for device-scoped models. **Partially done; single
   and multi-device device-scoped shard fetch now uses native NQE column
   filters, while deeper built-in NQE map parameterization remains future
   query-pushdown work.**
5. Add shard-scoped NQE for IPAM where safe. **Partially done for
   `ipam.ipaddress`, `ipam.prefix`, `ipam.vlan`, and `ipam.vrf`; hash/bucket
   grouping remains future work for cases without a stable filter column.**
6. Add bulk apply engine abstraction with adapter as the default engine.
   **Done.**
7. Implement conservative `bulk_orm` for simple models.
8. Continue TurboBulk/parquet work on the experimental branch.
9. Add chaos/scale validation as a release gate for scaling changes.

## Validation

Minimum for each implementation tranche:

```bash
invoke harness-check
invoke harness-test
invoke lint
invoke check
invoke scenario-test
invoke test
invoke docs
```

Before release:

```bash
invoke ci
```

Before declaring scale/recovery behavior production-ready:

- local Docker chaos/scale gate
- Playwright UI validation
- support bundle inspected for a forced failure
- live or synthetic large-dataset smoke with no committed customer identifiers

Current tranche evidence:

- `invoke playwright-test` passed after verifying sync-scoped execution runs,
  execution run detail, support/recovery controls, execution step fetch-mode
  visibility, and mobile sync-list overflow.
- `invoke ci` passed against the final tree, including harness checks,
  sensitive-content checks, linting, Docker build/start, Django system checks,
  scenario tests, 319 unit tests, Playwright UI validation, documentation build,
  and package build.
- Focused planner tests passed for single-device shard filters, multi-device
  `EQUALS_ANY` shard filters, and NQE diff shard filters.
- Focused branch-budget and planner tests passed for safe IPAM shard column
  filters on prefix, VLAN, and VRF keys.
- Focused execution-run API tests passed for support bundle, reconcile, and the
  stale-stage-without-branch retryability path.
- Focused execution-run API tests passed for explicit failed-branch discard and
  retry, including the guard that normal retry does not operate on partial
  branches.
- `invoke ci` passed after the explicit failed-branch discard/retry workflow was
  added.
- Focused synthetic scenario tests passed for merge-timeout reconciliation,
  duplicate terminal-stage claim protection, failed partial-branch discard, and
  support-bundle detail for those recovery paths.
- `invoke ci` passed after the synthetic chaos/recovery scenario coverage was
  added.
- Focused synthetic scenario tests passed after adding execution-run support
  bundle metrics for retry counts, fetch modes, apply engines, change counts,
  and stage/merge durations.
- `invoke ci` passed after execution-run support bundle metrics were added.
- Focused executor/synthetic tests passed after persisting execution-step
  `fetched_row_count` and `query_runtime_ms`, and the local Docker NetBox
  database was migrated through `0016_execution_step_query_metrics`.
- `invoke ci` passed after the safe IPAM shard filters and stale-stage reconcile
  recovery update.
- Live smoke planner ran against NetBox 4.5.9 on the common `dcim.site` and
  `dcim.device` models and completed in 16.76s with three planned branches.
- Live smoke planner ran against NetBox 4.6.0 on the same common models and
  completed in 12.99s with the same three-branch plan, giving a direct 4.6
  benchmark for the shared code line.
- `python manage.py makemigrations forward_netbox --check --dry-run` passed with
  no migration drift after adding execution-step apply-engine metadata.
- Focused Django tests passed for planner, fast bootstrap, execution-run API,
  and ingestion log export coverage after the apply-engine boundary was added.
- `invoke lint` and `invoke check` passed after import/format hooks normalized
  the tree.
- `invoke ci` passed after the shard-scoped fetch and adapter apply-engine
  boundary updates.
- `invoke makemigrations`, `invoke scenario-test`, `invoke check`,
  `invoke lint`, `invoke docs`, focused execution-run/API/log-export tests, and
  `invoke test` passed after adding execution-step row counters,
  reconciliation event history, and local migrations `0017` and `0018`.
- Local Docker NetBox database migrated through
  `0018_execution_run_reconciliation_events`.
- `invoke ci` passed after adding the execution-run recovery recommendation to
  support bundles and the native execution-run detail view.
- `invoke ci` passed after adding sync-scoped support bundle export and updating
  Playwright to verify the new sync detail control.
- Focused health/log-export tests passed after adding the read-only sync Health
  tab, including a guard that health summary rendering does not instantiate the
  Forward API client and a health check for enabled models without active NQE
  map coverage.
- `invoke playwright-test` passed after adding sync Health tab coverage for
  health summary, query binding, diff eligibility, next-run expectation, and
  health-detail rendering.
- `invoke ci` passed after adding the sync Health tab, support-bundle health
  summary, map-coverage diagnostics, documentation, and Playwright coverage.
- Focused synthetic scenario coverage passed after adding plain-language fetch
  explanations to execution-run support bundles and per-step metrics.
- `invoke ci` passed after adding execution-run support-bundle fetch
  explanations.
- Focused query-binding, health, and log-export tests passed after adding local
  query-drift classification to the Health tab and support bundle.
- `invoke playwright-test` passed after adding Local Query Drift coverage to the
  sync Health tab.
- `invoke ci` passed after adding local query-drift classification and Health
  tab rendering.
- `invoke lint`, focused health/log-export Django tests, and `invoke docs`
  passed after adding ledger-derived Capacity Projection to the Health tab.
- `invoke playwright-test` passed after adding Capacity Projection coverage to
  the sync Health tab.
- `invoke ci` passed after the Capacity Projection and long-term alignment
  documentation updates.
- `invoke lint`, focused query-binding/health/log-export Django tests,
  `invoke docs`, and `invoke playwright-test` passed after adding the explicit
  live query-drift export on the sync Health tab.
- `invoke lint`, focused health/query-binding/log-export Django tests,
  `invoke docs`, and `invoke playwright-test` passed after adding the explicit
  live source reachability export on the sync Health tab.
- `invoke ci` passed after adding explicit live source and live query-drift
  exports to the sync Health tab.
- `invoke lint`, focused health/query-binding/log-export Django tests,
  `invoke docs`, and `invoke playwright-test` passed after adding explicit
  live data-file freshness export to the sync Health tab and UI harness.
- `invoke ci` passed after adding explicit live data-file freshness export to
  the sync Health tab.
- Focused ingestion-merge and jobs tests passed after adding ledger fallback
  for merge eligibility, merge completion, next-stage auto-queue
  reconstruction, durable branch evidence after cleanup, and duplicate
  merge-claim guards.
- `invoke scale-chaos-test`, `invoke lint`, `invoke harness-check`,
  `invoke docs`, and `invoke ci` passed after the ledger-first merge/recovery
  hardening updates.
- Focused synthetic scenario coverage passed for retry-current-step
  reconstruction from execution-ledger state when `_branch_run` compatibility
  JSON is absent.
- `invoke ci` passed after the missing-JSON retry coverage and architecture
  plan status updates.
- Focused sync-state coverage passed for upgrading an old `_branch_run`
  payload into execution-ledger records and linking the compatibility cache to
  the new run.
- Focused synthetic/API coverage passed for idempotent retry-current-step
  handling once a step is already queued.
- Focused sync-state coverage passed for idempotent final run completion.
- Focused synthetic scenario coverage passed for locked discard-and-retry
  idempotency.
- `invoke ci` passed after old `_branch_run` upgrade coverage and locked
  retry/discard/finalize transition hardening.
- Focused sync-state coverage passed for ledger-derived sync display,
  workload, execution, and activity summaries when `_branch_run` JSON is absent.
- Focused sync-runner coverage passed for row-apply progress heartbeats updating
  execution-step counters and activity text when `_branch_run` JSON is absent.
- Focused log-export coverage passed for ingestion and sync support bundles
  exporting ledger-derived branch-run state when `_branch_run` JSON is absent.
- `invoke scale-chaos-test` includes log-export/support-bundle coverage.
- Focused sync-state/log-export coverage passed after making ledger-derived
  branch-run exports deterministic and adding explicit synthesized-state source
  labels to sync and ingestion support bundles.
- `invoke lint`, `invoke docs`, `invoke scale-chaos-test`, and `invoke ci`
  passed after the deterministic ledger-derived export/provenance update.
- `invoke test -- forward_netbox.tests.test_health` passed after adding
  structured Health `next_run.blockers` for fixed snapshots, missing
  baseline-ready ingestions, and raw-query maps that cannot use Forward diffs.
- `invoke ci` passed after the structured Health blocker update.
- `invoke test -- forward_netbox.tests.test_synthetic_scenarios` passed after
  adding support-bundle evidence coverage for an old completed run after a later
  run starts on the same sync.
- Focused synthetic/log-export coverage passed after adding linked-ingestion
  support details and sanitized issue samples to execution-run support bundles.
- Focused synthetic coverage passed after adding execution-run support-bundle
  bottleneck classification for Forward query versus Branching merge timing.
- `invoke ci` passed after the support-bundle ingestion-detail and bottleneck
  metric updates.
- Focused synthetic coverage passed after preventing duplicate stage workers
  from reclaiming an already running execution step.
- `ruff check forward_netbox/utilities/execution_ledger.py
  forward_netbox/tests/test_synthetic_scenarios.py forward_netbox/tests/test_jobs.py`,
  the focused timeout/duplicate-claim tests, `invoke lint`, `invoke docs`,
  `invoke scale-chaos-test`, and `invoke ci` passed after tightening the
  running-step claim guard to preserve legitimate timeout bookkeeping while
  blocking duplicate job-owner replacement.
- `ruff check forward_netbox/tests/test_synthetic_scenarios.py
  forward_netbox/utilities/execution_ledger.py` and `invoke test --
  forward_netbox.tests.test_synthetic_scenarios.ExecutionLedgerConcurrencyTest.test_simultaneous_stage_claim_allows_only_one_owner`
  passed after adding the transaction-backed simultaneous stage-claim stress
  test.
- `invoke ci` passed after the simultaneous stage-claim stress test was added
  and import ordering was normalized by the project hooks.
- `ruff check forward_netbox/tests/test_synthetic_scenarios.py
  forward_netbox/utilities/execution_ledger.py` and `invoke test --
  forward_netbox.tests.test_synthetic_scenarios.ExecutionLedgerConcurrencyTest`
  passed after adding transaction-backed simultaneous merge, retry, discard,
  and finalize stress coverage.
- `invoke ci` passed after the full transition concurrency matrix was added,
  including pre-commit, Docker build, Django checks, Playwright UI harness,
  packaging, `invoke scale-chaos-test`, and the 381-test Django suite.
- `ruff check forward_netbox/tests/test_synthetic_scenarios.py` and `invoke
  test --
  forward_netbox.tests.test_synthetic_scenarios.SyntheticSyncScenarioHarnessTest.test_support_bundle_includes_sanitized_model_issue_samples`
  passed after adding representative sanitized support-bundle issue fixtures for
  cabling, modules, virtual chassis, IPAM, and routing models.
- `invoke ci` passed after the representative sanitized support-bundle issue
  fixtures were added, including pre-commit, Docker build, Django checks,
  Playwright UI harness, packaging, `invoke scale-chaos-test`, and the 382-test
  Django suite.
- `ruff check forward_netbox/tests/test_synthetic_scenarios.py` and `invoke
  test --
  forward_netbox.tests.test_synthetic_scenarios.SyntheticSyncScenarioHarnessTest.test_failed_run_bundle_stays_actionable_without_branch_run_state`
  passed after adding failed-run support-bundle coverage with cleared
  compatibility state, errored job detail, sanitized issue detail, and a retry
  recommendation.
- `invoke ci` passed after the failed-run support-bundle coverage was added,
  including pre-commit, Docker build, Django checks, Playwright UI harness,
  packaging, `invoke scale-chaos-test`, and the 383-test Django suite.
- `ruff check forward_netbox/utilities/query_binding.py
  forward_netbox/tests/test_query_binding.py forward_netbox/tests/test_health.py`,
  `invoke test -- forward_netbox.tests.test_query_binding
  forward_netbox.tests.test_health`, `invoke lint`, `invoke docs`,
  `invoke harness-check`, and `invoke ci` passed after adding local
  raw/latest/pinned query commit guidance, requested commit revision in live
  query-drift export, and Health UI coverage.
- `ruff check forward_netbox/utilities/branch_budget.py
  forward_netbox/tests/test_sync.py`, `invoke test --
  forward_netbox.tests.test_sync.BranchBudgetTest.test_shard_fetch_contracts_cover_all_supported_models
  forward_netbox.tests.test_health.ForwardSyncHealthTest.test_sync_health_summary_reports_local_state`,
  `invoke docs`, and `invoke harness-check` passed after making fetch contracts
  report schema contract and local safety-filter guarantees for every
  supported model.
- `invoke ci` passed after the query commit-guidance and fetch-contract
  schema/safety updates, including pre-commit, Docker build, Django checks,
  Playwright UI harness, packaging, `invoke scale-chaos-test`, and the
  385-test Django suite.
- `invoke ci` passed after tightening public pending/merge gates to prefer
  execution-ledger state over stale `_branch_run` compatibility JSON and after
  passing `NETBOX_VER` into the development runtime containers. The same tree
  also passed `NETBOX_VER=v4.6.0 invoke check`, proving the shared compose
  harness boots NetBox 4.6 without loading the 4.5-only routing/peering
  plugins, then was restored to the default 4.5.9 runtime.
- `NETBOX_VER=v4.6.0 invoke build`, `NETBOX_VER=v4.6.0 invoke start`, and
  `NETBOX_VER=v4.6.0 invoke check` passed on 2026-05-16 after the
  query-pushdown and stale-worker recovery tranches. Runtime verification
  showed NetBox `4.6.0-Docker-5.0.1` loading only `forward_netbox` and
  `netbox_branching`. The local stack was then restored to
  `4.5.9-Docker-4.0.2` with `forward_netbox`, `netbox_branching`,
  `netbox_routing`, and `netbox_peering_manager`.
- `ruff check forward_netbox/utilities/resumable_branching.py
  forward_netbox/utilities/multi_branch_executor.py
  forward_netbox/tests/test_sync.py forward_netbox/tests/test_synthetic_scenarios.py`
  and four focused Django tests passed after adding ledger-only plan-item
  reconstruction for late-shard resume and guarding completed historical
  ledgers from suppressing a new run's preflight.
- `ruff check forward_netbox/utilities/multi_branch_planner.py
  forward_netbox/tests/test_sync.py` and the focused executor regression
  `ForwardMultiBranchExecutorAdaptiveSplitTest.test_run_next_plan_item_uses_ledger_shard_scope_for_native_fetch`
  passed after preserving single-shard scope on scoped fetch plans.
- `ruff check forward_netbox/utilities/resumable_branching.py
  forward_netbox/utilities/execution_ledger.py
  forward_netbox/utilities/multi_branch_executor.py
  forward_netbox/tests/test_sync.py` and four focused executor regressions
  passed after making direct planned Branching runs persist the execution
  ledger before shard apply and preventing fallback plan-item updates from
  mutating completed historical ledgers.
- `invoke scale-chaos-test`, `invoke docs`, `git diff --check`, and
  `invoke ci` passed on 2026-05-16 after the ledger-only late-shard resume,
  direct planned-run ledger persistence, historical-ledger mutation guard, and
  merge-timeout ordering updates. The full gate included harness checks,
  sensitive-content scanning, pre-commit, Docker build/start, Django checks,
  the 92-test scale/chaos gate, the 424-test Django suite, Playwright UI
  harness, docs, and package build.

## Rollback

Keep each workstream independently reversible:

- Shard-scoped fetch can fall back to model/full fetch by clearing fetch mode or
  disabling shard-capable query parameters.
- Bulk apply can fall back to adapter engine per model or globally.
- Recovery UI/API actions can be hidden while preserving ledger read-only state.
- Ledger records should remain readable even if orchestration temporarily falls
  back to compatibility `_branch_run` state.

## Decision Log

- Chosen: shard-scoped NQE before broad bulk write optimization because repeated
  full-model fetch is the biggest Branching-specific cost after resumability.
- Chosen: apply-engine abstraction before TurboBulk mainline adoption because
  current NetBox versions and model complexity vary by deployment.
- Chosen: recovery/reconcile actions before aggressive parallelism because
  correctness and operator control matter more than running more branches at
  once.
- Rejected: making fast bootstrap the default for every large sync because it
  removes branch review.
- Rejected: storing planned raw rows in the ledger because it duplicates Forward
  data and increases sensitive-data risk.
- Rejected: pushing shard filters into Python after full fetch because that does
  not solve the repeated Forward query/runtime problem.
- Chosen: durable run/step evidence over live sync-state export because support
  bundles must survive run completion, branch cleanup, and compatibility-state
  retirement.
- Chosen: explicit ledger transition guards over broad worker parallelism
  because correctness under duplicate callbacks and stale jobs is the gating
  requirement for self-service recovery.
