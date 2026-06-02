# Validation Matrix

Run the smallest gate that proves the change, then run the release gate before publishing.

| Change type | Required validation |
| --- | --- |
| Documentation only | `invoke harness-check`, `invoke harness-test`, `invoke docs` |
| Query map or NQE helper change | `invoke harness-check`, `invoke harness-test`, `invoke lint`, `invoke test`, update built-in NQE reference |
| Forward API client change | `invoke lint`, `invoke check`, `invoke test` |
| Sync planning or branch budget change | `invoke lint`, `invoke check`, `invoke scenario-test`, `invoke test`, local Docker sync smoke test |
| Branching ledger, recovery, or scale behavior change | `invoke lint`, `invoke check`, `invoke scenario-test`, `invoke scale-chaos-test`, `invoke test`, `invoke playwright-test` when UI/API surfaces change; `scale-chaos-test` includes focused recovery and support-bundle export coverage, and `invoke scale-benchmark` should be run against the relevant execution run when runtime evidence exists |
| Validation or drift policy change | `invoke lint`, `invoke check`, `invoke scenario-test`, `invoke test`, `invoke playwright-test`, validation-only smoke test, force-allow override smoke test when the change adds a break-glass path |
| NetBox model adapter change | `invoke lint`, `invoke check`, `invoke scenario-test`, `invoke test`, targeted local Docker sync |
| UI/API workflow change | `invoke lint`, `invoke check`, `invoke test`, `invoke playwright-test`, browser/UI verification when visible behavior changes |
| Release | `invoke ci`, GitHub CI success on `main` and tag |

## Core Commands

```bash
invoke harness-check
invoke harness-test
invoke lint
invoke check
invoke scenario-test
invoke scale-chaos-test
invoke test
invoke playwright-test
invoke release-dataset-gate --dataset-label=redacted
invoke docs
invoke package
invoke ci
```

Do not run Django test tasks against the shared local Docker runtime while a
Forward execution run is active. `invoke test`, `invoke scenario-test`, and
`invoke ingestion-delete-regression` fail fast when a queued/running/waiting
execution run exists because shared RQ registries can disturb live ingestion
jobs. Use an isolated stack for tests, finish or stop the ingestion first, or
set `FORWARD_NETBOX_ALLOW_SHARED_RUNTIME_TESTS=1` only for an intentional
bypass.

CI-style tasks such as `invoke test-ci`, `invoke scenario-test-ci`, and
`invoke scale-chaos-test` run against the shared runtime only when the active-run
guard can inspect it and finds no active execution runs. `invoke playwright-test`
uses the same guard for the deterministic UI harness. If the guard detects an
active run or cannot inspect the shared runtime, for example because local
Postgres is saturated, these tasks run against an isolated compose project
instead of bypassing the shared-runtime safety check.

Use `invoke test-isolated` directly when a live ingestion is active or when you
want the full Django regression suite in the separate `forward-netbox-test`
compose project. It preserves that project's Postgres volume by default for
faster repeat `--keepdb` runs.

## Destructive Chaos Harness (Opt-In)

Use the destructive worker-kill harness only for deferred-risk recovery proof.
It is intentionally excluded from `invoke ci`.

```bash
invoke docker-chaos-kill --scenario=stage-before-branch --confirm
invoke docker-chaos-kill --scenario=stage-after-branch --confirm
invoke docker-chaos-kill --scenario=stage-during-apply --confirm
invoke docker-chaos-kill --scenario=merge-during-exec --confirm
```

Optional scenario-aware controls:
- `FORWARD_CHAOS_SYNC_NAME=<sync-name>` waits for scenario readiness on the active execution run.
- `FORWARD_CHAOS_OUTPUT_DIR=/tmp/chaos` exports execution-run support bundles for post-kill evidence.
  The chaos task now also validates that the newest exported bundle contains run metadata,
  at least one step, and a recognized recovery action aligned to the requested scenario.
  It also verifies scenario-specific step-state evidence (for example branch linkage for
  `stage-after-branch`, row-progress counters for `stage-during-apply`, and merge-job linkage for
  `merge-during-exec`).
  The same output directory also receives `chaos-<scenario>-metadata-*.json`
  with the killed worker/container ID, restored worker count, execution run ID,
  active step ID, active step job ID, branch ID/name when present, recovery
  action, and whether support-bundle recovery validation passed.
- `FORWARD_CHAOS_WAIT_SECONDS` / `FORWARD_CHAOS_POLL_SECONDS` tune readiness polling.

## Live Pushdown Proof (Opt-In)

Use this command to gather live runtime/parity evidence for shard pushdown on a
specific model using an existing sync configuration:

```bash
invoke pushdown-profile --sync-name "ui-harness-sync" --model "dcim.interface"
```

Optional flags:
- `--query-name "Forward Interfaces"` when multiple maps are bound to the model.
- `--sample-shard-keys 500` to widen shard sample size.
- `--output-json /tmp/pushdown-profile.json` to persist the report artifact.

### Architecture Audit Artifact

Use this to emit a single JSON artifact that captures the current apply-engine
model matrix (bulk-ORM safe set + adapter-required blockers) and optional
sync/runtime evidence:

```bash
invoke architecture-audit
invoke architecture-audit --sync-name "ui-harness-sync" --output-json /tmp/architecture-audit.json
invoke architecture-audit --fail-on-gap
invoke architecture-audit-check
invoke architecture-runtime-evidence
invoke architecture-completion-audit --output-json /tmp/architecture-completion-audit.json
invoke release-runtime-preflight --dataset-label=redacted
invoke field-scale-runtime-matrix --resume=False
invoke release-dataset-gate --dataset-label=redacted
invoke release-readiness-audit --dataset-label=redacted
invoke execution-run-recovery --sync-name "ui-harness-sync" --skip-reconcile=True
invoke prune-compat-cache --dry-run=True --output-json docs/03_Plans/evidence/compat-cache-prune.json
invoke runtime-capacity-review --source-name "ui-harness-source"
invoke scale-benchmark --sync-name "ui-harness-sync" --output-json docs/03_Plans/evidence/scale-benchmark.json
invoke sync-health-gate --sync-id 50 --max-polls 180 --interval-seconds 30
invoke sync-health-gate --sync-id 50 --max-polls 10 --interval-seconds 30 --allow-nonterminal
invoke sync-health-monitor --sync-ids 50,51 --max-polls 6 --interval-seconds 30 --allow-nonterminal --output-json docs/03_Plans/evidence/sync-health-monitor.json
invoke sync-autorecover-monitor --sync-ids 50,51 --max-polls 6 --interval-seconds 30 --allow-nonterminal --include-all-ingestions --fail-on-recovery --output-json docs/03_Plans/evidence/sync-autorecover-monitor.json
```
- `invoke architecture-runtime-evidence` writes
  `docs/03_Plans/evidence/architecture-runtime-evidence.json`. Completion audit
  consumes it via `--runtime-evidence` (default path) and marks runtime checks
  complete when the evidence is fresh and passed.
  Runtime evidence now also records a compatibility-cache retirement dry-run
  report at `docs/03_Plans/evidence/compat-cache-prune-runtime.json`.
- Use `invoke architecture-runtime-evidence --run-field-scale` to execute the
  approved field-scale runtime matrix when `FORWARD_SMOKE_USERNAME`,
  `FORWARD_SMOKE_PASSWORD`, and `FORWARD_SMOKE_NETWORK_ID` are set in the
  environment.
  The same matrix can be run independently with
  `invoke field-scale-runtime-matrix --resume=False` for release proof. Use
  `--step <matrix-step-name>` to run one long step at a time; the artifact is
  marked `partial` until every required step has passed.
  The matrix records per-step timeout evidence instead of dropping the whole
  runtime artifact; tune with `FORWARD_SMOKE_STEP_TIMEOUT_SECONDS` when the
  approved dataset legitimately needs longer query planning time.
  It also writes incremental sanitized step evidence to
  `docs/03_Plans/evidence/field-scale-runtime-matrix.json` by default. Override
  with `FORWARD_FIELD_SCALE_EVIDENCE_PATH` for local scratch runs.
  Set `FORWARD_SMOKE_DATASET_LABEL=redacted` before `field-scale-runtime-matrix`
  when capturing `1.1.x` release evidence so the dataset gate can enforce the
  expected dataset lineage.
  When `--run-field-scale` is omitted, runtime evidence reuses this artifact if
  it exists and is fresh, so a completed field-scale matrix can be folded into
  the completion audit without rerunning the matrix.
  Scope exploratory evidence with `FORWARD_SMOKE_MODELS` only when the reduced
  model set is called out in the evidence notes; do not use a narrow model set
  to claim full field-scale completion.
- Add `--scale-sync-name <sync-name>` when the field-scale benchmark should use
  a large sync other than the local chaos probe sync. The default
  `ui-harness-sync` path is useful for wiring checks but is intentionally too
  small to close the fallback or scheduler runtime-evidence gates.
- Add `--capacity-source-name <source-name>` when runtime evidence should record
  the source's current query-fetch/page-size settings alongside local worker,
  host, and PostgreSQL tuning guidance. The same review can be generated
  directly with `invoke runtime-capacity-review`.
- Add `--capacity-query-fetch-concurrency <count>` and
  `--capacity-nqe-page-size <count>` with `--capacity-source-name` when runtime
  evidence should reapply source fetch tuning after harness seed/reset before
  capacity review. This keeps source tuning evidence aligned with the measured
  run profile.
- Add `--capacity-worker-replicas <count>` when runtime evidence should scale
  and preserve a non-default `netbox-worker` count through chaos probes and
  capacity review. This prevents `docker compose up` or chaos restore from
  silently testing the compose default worker count after local tuning.
- Use `--scale-input-json /path/to/sanitized-support-bundle.json` when support
  is validating an exported large-run support bundle offline. Use
  `--scale-run-id <execution-run-id>` when the large run exists in the local
  NetBox database.
- Add `--scale-reconcile` with `--scale-run-id` or `--scale-sync-name` when a
  live run should be reconciled before benchmark export. Do not use it with
  `--scale-input-json`; offline bundles are read-only evidence.
- Add `--skip-chaos` when refreshing large-run scale/capacity evidence while a
  live ingestion is active. This reuses fresh prior destructive runtime
  evidence from the existing architecture evidence artifact and avoids compose
  reconciliation, harness reseeding, worker scaling, and worker-kill probes
  mid-run.
- Offline support-bundle evidence must pass configured sensitive-content
  scanning before it is accepted by `forward_scale_benchmark --input-json`.
  Put customer-local names, tenant labels, network IDs, and snapshot IDs in
  `.sensitive-patterns.local.txt` before evaluating field evidence.
- `--top-slow-models 5` to automatically profile the slowest recent models from execution-step history.
- `invoke scale-benchmark` evaluates the latest execution-run support bundle
  for a sync and emits a sanitized pass/warn/fail report for fallback rate,
  diff utilization, row failures, partition retries, throughput wait, and
  apply-engine evidence. Use `--run-id` for a specific run or `--input-json`
  to evaluate an exported support bundle offline. Use `--reconcile` only for
  live `--sync-name` or `--run-id` selectors when support intentionally wants
  to repair stale ledger state before benchmarking. Scale evidence is rejected
  when a run is marked completed but still contains non-terminal steps, because
  that cannot prove fallback or scheduler readiness.
- `invoke execution-run-recovery` reports the latest execution-run recovery
  recommendation for a sync or run ID. Add `--enqueue-next=True` only when
  support intentionally wants to resume the next eligible Branching shard
  through the native NetBox job queue after reconciliation.
- `invoke sync-health-gate` runs `forward_watch_sync`,
  `forward_blocker_audit`, and `forward_warning_audit` in one loop and fails
  immediately when blockers/warnings/errors appear. Use
  `--allow-nonterminal` for mid-run health checks on long field-scale syncs.
- `invoke sync-health-monitor` extends the same checks across multiple sync IDs
  and writes polling evidence to JSON. Use it for long customer-dataset soaks
  where support needs continuous proof that no blocker/warning/error regressions
  appeared between spot checks.
- `invoke sync-autorecover-monitor --fail-on-recovery` is the strict burn-in
  gate for release readiness: it fails when any auto-recovery action was
  required, even if blocker/warning/error counts stayed at zero.
- `invoke release-dataset-gate --dataset-label=redacted` is the strict `1.1.x`
  dataset gate: it fails when field-scale evidence is stale, not `passed`, not
  labeled with REDACTED's dataset, missing required matrix steps, or generated
  with `resume=True` (unless explicitly overridden with
  `--allow-resumed-artifact=True`).
- `invoke release-runtime-preflight --dataset-label=redacted` checks runtime
  prerequisites before the matrix starts: required smoke env vars, expected
  dataset label, and Docker reachability.
- `invoke release-readiness-audit --dataset-label=redacted` aggregates preflight,
  dataset gate, and architecture-completion gate in one JSON artifact for
  release sign-off evidence.
  Failed run entries now include `failure_code` / `failure_hint` so environment
  gates such as `docker_api_unreachable` are explicit instead of buried in raw
  command logs.

Operational default:
- keep source `query_fetch_concurrency` at `6` initially and raise only with
  observed DB/worker headroom.

For repeated operational soak runs (manual, opt-in):

```bash
invoke scale-soak --runs 3 --execution-backend branching --max-changes-per-branch 10000
```

## Sensitive-Content Gate

The sensitive-content guard must stay in local and CI validation:

```bash
python scripts/check_sensitive_content.py
python scripts/check_sensitive_content.py --all-history
```

Use `.sensitive-patterns.local.txt` for local-only customer names, tenant labels, network IDs, or other identifiers that should never be committed.
