# Quality Score

Last reviewed: 2026-07-18

Current score: **B**

## Strengths

- CI covers sensitive-content scanning, pre-commit, docs build, NetBox startup, Django checks, plugin tests, and packaging.
- Sync execution uses native NetBox Branching rather than a side-channel import path.
- Shipped NQE maps are committed and documented.
- Release workflow is repeatable across GitHub Releases and PyPI.

## Risks

- Core sync behavior is concentrated in large modules that are harder for humans and agents to modify safely.
- Local docs builds depend on dev dependencies that may not be installed outside Poetry or CI.
- Large-dataset behavior needs continued Docker/UI-path validation because branch change counts can differ from NQE row counts.
- Large native column-filter shard fetches now partition oversized `EQUALS_ANY`
  batches. Query-side shard parameters are deferred until each built-in map is
  converted to a live-validated `@query` form; otherwise stale repository maps
  can fail preflight with unbound parameter errors.
- The available query surface has not yet shown a safe deterministic hash/mod
  primitive for bucketed pushdown, so the remaining query-side speed work is
  still constrained by query-language support rather than plugin plumbing.
- Resumable Branching now has first-class execution run/step records, but old parameter-backed branch state is still kept as a compatibility/display cache fallback until ledger-derived UI and recovery behavior are proven across upgrades. Sync enqueue continuation is now ledger-only once a run exists; the remaining compatibility branch-state cache writes are compatibility/read-through only.
- The apply-engine boundary and per-model fallback reporting exist, and every
  supported model is explicitly classified as a future bulk candidate or
  adapter-required. Only the conservative adapter engine is active;
  high-throughput bulk engines still need separate validation before use.
- The sync Health tab now covers local-state diagnostics, local query-drift
  classification, and an explicit live query-drift export for repository/query
  ID source checks. It also shows raw/latest/pinned query commit behavior and
  exposes explicit live source reachability and data-file freshness exports.
- Documentation and tests must keep avoiding customer-derived identifiers.
- Plan lifecycle hygiene needs repair: the 2026-07-18 harness audit counted 102
  files under `docs/03_Plans/active/`, including long-running plans over 1,000
  lines. Active does not currently mean actionable with enough precision.
- Repository knowledge has structural and freshness gates, but recurring
  gardening currently detects drift rather than automatically preparing a
  cleanup pull request.

## Near-Term Improvements

- Triage active plans into current, blocked, completed, or superseded states;
  keep a small active index that points to detailed history rather than using
  the active directory as an archive.
- Extend the weekly harness-gardening signal into a reviewed cleanup pull
  request workflow once repository automation has an approved write identity.

- Add boundary tests before extracting sync adapters from `sync.py`.
- Continue moving UI summary and recovery decisions from compatibility branch-state JSON to the execution ledger.
  Sync display parameters, workload summary, execution summary, and activity
  text now use ledger-derived execution-ledger presentation when compatibility branch-state cache JSON is
  absent. Row-apply progress also updates active execution-step counters and
  heartbeat timestamps, not only compatibility JSON. Stage enqueue, merge
  continuation, stage-worker resume, and failure/progress reporting now prefer
  the execution ledger whenever a run exists. Runtime phase updates also
  persist to the execution ledger first, and failure reconciliation now uses
  the synthesized ledger display state when the cache is empty or stale.
  Fresh job-backed runs now create execution-run records without first writing
  a compatibility branch-state cache.
  The seeded UI harness fixture now resolves the execution run from ledger
  state instead of injecting `execution_run_id` into compatibility JSON.
  Support bundles now have a regression that covers upgrade from old branch
  state, cleanup, and later export after the branch is gone.
  Ledger-derived export state is now ordered deterministically and labeled so
  support can tell whether execution-ledger state came from compatibility JSON or
  execution-ledger evidence.
- Keep reconciliation event history covered as recovery actions evolve.
- Keep row-counter telemetry and per-model apply-engine fallback reporting
  covered as apply engines evolve beyond the adapter path. New supported models
  must be explicitly classified before they can pass the model contract tests.
- Keep health diagnostics covered as they expand across local state, live
  reachability, remote query-library source/commit drift, data-file freshness
  checks, fetch-contract reporting, and apply-engine reporting.
- Keep shard-fetch parity covered as it expands beyond the first supported
  model; every supported model now has a parity regression through the fetch
  path, `dcim.interface` and `ipam.prefix` still prove the narrow native column
  filters, `dcim.site` proves the safe full-fetch fallback keeps row shape
  under local filtering, and query-registry tests prove shipped built-in NQE
  does not reference unregistered shard parameters.
- Keep support bundles covered across cleanup and later-run handoff; the sync
  support bundle now survives cleanup plus a later run while still reporting
  execution-ledger provenance.
- Expand capacity projection from observed shard timing into pre-run branch-plan
  timeout risk once more historical execution data is available.
- Expand stale-job, stale-branch, merge, and duplicate-job chaos scenarios for repeated local sync tests and interrupted customer baselines. Deterministic stale-worker coverage now proves before-branch, after-branch, and merge recovery recommendations; true Docker worker-kill injection remains future work.
- Keep expanding regression tests around branch-budget planning, density tracking, and retry behavior.
- Implement shard-scoped fetch and later bulk apply only where the NQE and NetBox model contracts can prove the behavior is safe.

## Long-Term Alignment Items

These are the larger refactors still needed before the project should be
considered fully positioned for very large, self-service deployments:

### Former Deferrals (Now Implemented)

- Compatibility branch-state-cache runtime control-plane retirement is implemented in
  ledger-only orchestration paths.
- Live Forward runtime pushdown proof is captured with
  `forward_pushdown_profile` against a live source.
- Destructive Docker worker-kill coverage now includes scenario-aware readiness
  and support-bundle artifact capture.
- Faster apply engines now include an enabled `bulk_orm` lane for a narrow
  safe model set, with adapter fallback outside that set.

- Keep the completed long-term checklist in
  `docs/03_Plans/completed/2026-05-15-scale-hardening-remaining-work.md` under
  "Long-Term Completion Backlog" and "Remaining Long-Term Architecture
  Alignment"; do not duplicate detailed acceptance criteria here.
- Retire active orchestration dependence on the compatibility branch-state cache
  after ledger-derived UI, API, recovery, and support-bundle behavior are proven
  across upgrades.
  The active plan now defines the compatibility window and the tests required
  before removing active JSON writes.
- Define a per-model shard-fetch contract, including safe NQE filters,
  query-pushdown parameters where available, exact local safety filters, schema
  contract, and explicit fallback reasons. Fetch mode, schema contract, local
  safety-filter guarantee, and fallback reason are now reported for every
  supported model; query-side shard parameters are deferred until each built-in
  map is converted to a live-validated parameterized query. Live schema-parity
  fixtures, measured pushdown proof, and hash/bucket contracts remain future
  work.
- Add a conservative faster apply engine only below the existing Branching and
  fast-bootstrap lanes; do not add a separate bulk-sync product workflow. Keep
  `adapter` as the default until faster engines prove equivalent native NetBox
  validation, change tracking, Branching diff visibility, and row issue
  capture.
- Keep explicit health actions covered as they expand so operators can verify
  Forward-side state before starting long runs without making page render
  depend on live Forward API calls.
- Keep `invoke scale-chaos-test` as the focused synthetic release gate for
  Branching execution, recovery, shard planning, and apply-engine changes; add
  destructive worker-kill scenarios separately when the local harness can run
  them repeatably.
- Add durable evidence checks proving support bundles remain actionable after a
  run completes, branch cleanup runs, compatibility compatibility branch-state cache state is
  cleared, or a later run has started. Current execution-run bundle coverage
  includes branch cleanup and later-run evidence, while broader forced-failure
  export cases remain future work.
- Add concurrency/idempotency checks for stage, merge, retry, discard, and
  finalize transitions so duplicate callbacks and simultaneous workers cannot
  advance a ledger step twice.
- Keep main, NetBox 4.6, and experimental bulk branches aligned at the product
  workflow level, with branch-specific differences limited to runtime
  capabilities.

## Self-Service Target

A large deployment should be operable from native NetBox surfaces without
developer-assisted log archaeology. The remaining UX and support work should
make these answers visible before or during a run:

- why the next run is true diff, reconciliation, or full fallback
- which query bindings or data files are stale or unverified
- which shard, job, branch, or merge step currently needs action
- whether the bottleneck is Forward query runtime, row apply, Branching diff,
  Branching merge, worker timeout, or PostgreSQL cleanup
- which sanitized support bundle should be exported for support
- whether that support bundle still contains the required run evidence after
  cleanup or compatibility-state retirement
- whether linked ingestion issues are visible without exporting raw row/default
  payloads
- which measured runtime phase is currently largest in the execution-run bundle
