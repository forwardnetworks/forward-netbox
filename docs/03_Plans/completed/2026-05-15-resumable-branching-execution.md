# Resumable Branching Execution

## Goal

Make large Branching syncs resilient to NetBox worker timeouts by turning the
baseline workflow into a durable, resumable series of short native NetBox jobs
instead of one long-running background job.

The operator-visible outcome should be:

- A large Branching baseline can resume from the last completed shard after a
  worker timeout, restart, or transient failure.
- Each shard and merge step has its own job logs, status, and exportable support
  bundle.
- Fast bootstrap remains the recommended first-load path for trusted huge
  baselines, and a successful fast-bootstrap run can seed later Branching diffs
  on a newer snapshot.
- Branching remains available for reviewable first loads, but the UI clearly
  warns when the selected workload is too large to be practical as one
  operator-reviewed baseline.

## Constraints

- Stay NetBox-native: use NetBox jobs, plugin models, and `netbox_branching`
  branches rather than an external queue or sidecar runner.
- Stay Branching-native: keep one reviewable Branching branch per shard and use
  the existing merge lifecycle.
- Keep NQE as the source of truth for normalization and NetBox-shaped rows.
  Python execution may schedule, retry, apply, and merge rows, but must not add
  new data-normalization rules that diverge from the NQE contracts.
- Preserve the existing branch-budget contract. Do not solve timeouts by raising
  the change budget above the operator's Branching guidance.
- Do not persist customer identifiers, network IDs, snapshot IDs, credentials,
  or private row data in tests, docs, or plan fixtures.
- Keep the current fast-bootstrap backend available for trusted initial loads.
- Keep same-snapshot diff behavior explicit: fast bootstrap can seed future
  Branching diffs only when the next run is on a later snapshot and enabled maps
  have query IDs.

## Touched Surfaces

- `forward_netbox/models.py`
- `forward_netbox/jobs.py`
- `forward_netbox/utilities/multi_branch_executor.py`
- `forward_netbox/utilities/multi_branch_lifecycle.py`
- `forward_netbox/utilities/ingestion_merge.py`
- `forward_netbox/utilities/sync_state.py`
- `forward_netbox/utilities/ingestion_presentation.py`
- `forward_netbox/views.py`
- `forward_netbox/templates/forward_netbox/forwardsync.html`
- `forward_netbox/templates/forward_netbox/forwardingestion.html`
- `forward_netbox/tests/test_sync.py`
- `forward_netbox/tests/test_jobs.py`
- `forward_netbox/tests/test_ingestion_merge.py`
- `forward_netbox/tests/test_models.py`
- `forward_netbox/tests/test_issue_rendering.py`
- `scripts/playwright_forward_ui.mjs`
- `docs/01_User_Guide/README.md`
- `docs/01_User_Guide/configuration.md`
- `docs/01_User_Guide/troubleshooting.md`

## Approach

### 1. Persist a Durable Branching Plan

The current Branching path can still behave like:

```text
one RQ job -> plan -> shard 1 -> merge -> shard 2 -> merge -> ... -> done
```

That puts the whole baseline inside one NetBox worker timeout window. The new
shape should persist the plan and make the coordinator job short:

```text
coordinator job -> resolve snapshot -> validate -> build plan -> store plan
```

Persist enough state on `ForwardIngestion.branch_run` to resume without
replanning:

- `phase`
- `phase_message`
- `snapshot_id`
- `validation_run_id`
- `plan_preview`
- `plan_items` or a compact persisted plan reference
- `next_plan_index`
- `total_plan_items`
- per-item status: pending, staging, staged, merging, merged, failed, skipped
- last job ID and last error metadata

Do not store raw customer rows in `branch_run`. Store plan metadata and rerun the
model query/diff for the current shard when the shard job executes.

### 2. Split Execution Into Short Jobs

Introduce explicit job entrypoints for:

- plan/coordinator
- stage one shard
- merge one shard
- finalize baseline

The coordinator creates or updates a `ForwardIngestion`, records validation
results, stores the plan, then enqueues the first shard-stage job.

Each shard-stage job:

- loads the persisted plan
- claims the current plan item
- creates or reuses the native Branching branch for that shard
- fetches the needed model rows through the existing query-fetch path
- applies rows through the existing NetBox adapters
- writes row statistics, issues, heartbeat, and model results
- marks the item staged
- enqueues the merge job when auto-merge is enabled, otherwise leaves the item
  awaiting operator merge/review

Each merge job:

- merges only the shard branch for the current item
- records merge statistics and logs
- deletes/cleans up the branch after successful merge using the existing
  lifecycle behavior
- advances `next_plan_index`
- enqueues the next shard-stage job

The finalizer marks the ingestion complete and baseline-ready only after the
last required shard is merged or explicitly completed according to the selected
workflow.

### 3. Add Resume and Requeue Controls

Expose native UI actions on the sync/ingestion detail pages:

- Resume from next pending shard
- Retry failed shard
- Requeue merge for staged shard
- Export logs

These should operate on the persisted ingestion state. They should not require
the operator to start a new sync and should not discard completed shards.

When a job times out, the timeout handler should record the timeout issue and
leave the item in a retryable state. A later resume should retry the current
item rather than rebuilding the whole baseline.

### 4. Make Fast Bootstrap the Recommended Huge-Baseline Seed

For very large trusted first loads, the recommended operator path should be:

```text
fast bootstrap -> baseline_ready=True -> later snapshot -> Branching diff
```

The UI should make this explicit:

- When the plan preview estimates a very large Branching baseline, warn that a
  single Branching baseline may exceed worker timeouts even with bounded shards.
- Recommend fast bootstrap for trusted first loads.
- Explain that fast bootstrap can seed later Branching diffs only on a newer
  snapshot and only when maps resolve to query IDs.
- Keep Branching selectable for reviewable initial baselines.

This is a workflow guidance change, not a data-contract change. The same NQE
maps and NetBox model adapters remain in use.

### 5. Improve Timeout and Progress Visibility

Use the 0.9.0 log exporter as the support handoff surface. Extend exported logs
to include the durable plan state once implemented:

- current plan index
- current model
- current shard key or branch name
- current stage: planning, staging, merge, waiting, finalizing
- last heartbeat timestamp
- last job ID
- last error type
- retry count

The ingestion and sync summaries should distinguish:

- full baseline
- fast-bootstrap baseline
- Branching diff
- same-snapshot run that cannot diff
- waiting for manual merge
- retryable timeout

## Validation

Minimum test coverage:

- Coordinator persists a plan and exits without applying all shards inline.
- Shard-stage job applies exactly one plan item and advances item state.
- Merge job merges exactly one branch and enqueues the next item.
- Timeout during staging records an issue and leaves the item retryable.
- Timeout during merge records an issue and leaves the merge retryable.
- Resume continues from `next_plan_index` and does not rerun completed shards.
- Retry failed shard reuses the same plan item and preserves previous issues.
- Manual-review mode stops after staging and waits for operator merge/requeue.
- Fast-bootstrap baseline can seed a later Branching diff when query IDs exist.
- Same-snapshot Branching run does not incorrectly diff against the fast
  bootstrap seed.
- Log export includes plan state, current shard, merge job, and retry metadata.

Required gates:

```bash
invoke harness-check
invoke harness-test
invoke lint
invoke check
invoke scenario-test
invoke test
invoke playwright-test
invoke docs
invoke ci
```

Additional local validation:

- Run a synthetic multi-shard sync and force a staged timeout/failure on shard
  N; verify resume starts at shard N rather than shard 1.
- Run a synthetic merge failure and verify `Export Logs` contains both sync and
  merge job details.
- Run the ORG-sized dataset or equivalent live smoke with a deliberately low
  worker timeout to prove the run is recoverable.

## Rollback

Keep the existing single-job Branching executor available behind an internal
compatibility switch until the durable state machine is validated. Rollback is:

- disable resumable Branching orchestration
- use the existing `MultiBranchExecutor.run()` path
- keep any new read-only state fields ignored by the old path

If a resumable run is mid-flight during rollback, leave its ingestion visible
and exportable, but require starting a new sync on the old executor path. Do not
try to translate partially completed persisted plans back into a single running
job.

## Decision Log

- Rejected: solve repeated 16-hour timeouts by only increasing
  `RQ_DEFAULT_TIMEOUT`. That makes the failure window larger but still loses the
  whole job when the ceiling is reached.
- Rejected: raise branch budgets above Branching guidance. The branch budget is
  an operational safety constraint, not a performance knob.
- Rejected: external runner/sidecar. NetBox jobs and plugin model state are
  sufficient, and staying native keeps permissions, logs, and operator workflow
  inside NetBox.
- Rejected: store raw Forward rows in NetBox to resume. Persist plan metadata
  and refetch per shard instead; this avoids storing customer inventory payloads
  and keeps NQE as the source of truth.
- Chosen: make fast bootstrap the recommended huge first-load seed while keeping
  Branching as the reviewable steady-state and diff path.

## Completion Evidence

- Added durable Branching plan metadata under the existing sync `branch_run`
  state, including plan item status, stage job ID, merge job ID, retry count,
  shard metadata, and current phase.
- Added a coordinator-to-shard execution path where job-backed Branching syncs
  persist the plan and enqueue one shard-stage NetBox job instead of applying
  all shards inline.
- Added a shard-stage job entrypoint that refetches only the current model,
  selects the persisted shard by metadata, stages one Branching branch, and
  leaves the current item retryable on timeout/failure.
- Added merge chaining so successful auto-merge jobs enqueue the next shard and
  manual review flows can continue from the persisted plan index.
- Added retryable timeout state for stage and merge failures.
- Extended log export with a `branch_plan` section and included durable plan
  state in the existing support bundle.
- Added UI guidance for large Branching baselines and kept `Export Logs`,
  `Continue Ingestion`, and ingestion merge/requeue controls inside the native
  NetBox views.
- Documented resumable Branching behavior, timeout handling, and fast-bootstrap
  seed guidance.

Validation:

```bash
invoke lint
invoke check
invoke scenario-test
invoke test
invoke ci
```

`invoke ci` passed after the final implementation.
