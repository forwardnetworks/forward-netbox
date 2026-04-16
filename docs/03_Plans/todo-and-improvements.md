# Forward NetBox TODO And Improvements

## Purpose

This document captures the next major improvement phase for `forward_netbox` after the current `v0.1.4` release line.

The plugin is already in a solid release state:

- NetBox `4.5.x` only
- single migration
- single-commit release discipline
- branch-backed sync, diff, and merge flow
- Forward snapshot-aware execution
- NQE-driven model ingestion

The next phase should make the plugin materially better than the current baseline by adding stronger validation, richer snapshot intelligence, clearer operator visibility, and better release/quality automation.

## Core Principles

- Keep NQE authoring inside Forward, not in the plugin.
- Keep the plugin NQE-first: Python should validate identity, persist rows, and enforce policy, not reshape inventory output.
- Keep branch-backed review and merge as the default safety model.
- Keep manual merge as the default operator flow, but make preflight validation and merge gating first-class.
- Prefer reusable policy objects over ad hoc per-sync flags where the workflow becomes more complex.
- Preserve the current clean-release discipline: one migration, one release tag, deterministic assets.

## Non-Goals

- Do not add a query builder, transform-map editor, or general NQE authoring experience inside NetBox.
- Do not move data-shaping logic back into Python.
- Do not redesign the current source, sync, ingestion, and merge architecture unless a specific improvement requires a narrow extension.
- Do not broaden supported NetBox versions beyond `4.5.x` in this phase.

## High-Level Outcomes

This phase should deliver the following outcomes:

1. Operators can validate a sync before a branch is created.
2. Operators can compare a target snapshot against a baseline snapshot with persisted results.
3. Operators can see why a sync was allowed, blocked, or force-merged.
4. Built-in NQE execution contracts are tested and protected from accidental drift.
5. CI, packaging, and release steps are automated enough to reduce manual mistakes.
6. The roadmap for remaining object-model parity and richer Forward-native capabilities is explicit.

## Workstreams

### 1. Snapshot Intelligence And Drift Policy

Add a reusable policy object that defines how preflight comparison and merge gating should behave.

#### New object

- `Forward Drift Policy`

#### Why

Today snapshot selection exists, but there is no reusable operator policy for:

- what snapshot to compare against
- what health checks are required
- what kinds of drift or destructive changes should block progress

That logic should not live as scattered sync flags.

#### Proposed policy fields

- `name`
- `description`
- `target_snapshot_mode`
  - use the sync snapshot selector as-is
- `baseline_mode`
  - `previous_merged_ingestion`
  - `explicit_snapshot`
  - `none`
- `baseline_snapshot_id`
  - only valid when `baseline_mode=explicit_snapshot`
- `require_processed_snapshot`
- `block_on_snapshot_health_regression`
- `block_on_query_errors`
- `block_on_zero_rows_for_enabled_model`
- `block_on_destructive_change_threshold`
- `max_deleted_objects`
- `max_deleted_objects_percent`
- `enabled`

#### Expected behavior

- Each `Forward Sync` references one drift policy.
- The default baseline is the snapshot from the most recent successfully merged ingestion for that sync.
- If there is no valid baseline, the policy may either permit validation with no comparison or block based on configured rules.
- Policy evaluation is persisted and auditable.

### 2. Validation Runs As A First-Class Workflow

Add a dedicated preflight workflow that resolves snapshots, validates query output, computes drift summaries, and decides whether ingestion may proceed.

#### New object

- `Forward Validation Run`

#### Why

Today the operator sees the result after ingestion has already started. A better workflow is:

1. resolve target snapshot
2. resolve baseline snapshot
3. validate enabled models
4. calculate preflight risk
5. decide whether branch creation should happen

#### Validation run lifecycle

1. Resolve the target snapshot.
2. Resolve the baseline snapshot from the drift policy.
3. Collect snapshot metadata and health metrics for target and baseline.
4. Execute read-only validation checks for the enabled NetBox models.
5. Record row counts, query failures, and missing-model results.
6. Compute drift and risk summaries.
7. Persist the result before any branch is created.
8. If the run passes policy, allow the normal ingestion flow to continue.
9. If the run fails policy, stop before branch creation and surface the blocking reasons clearly.

#### Execution entry points

- standalone `Run Validation` action from the sync detail page
- automatic preflight phase of `Adhoc Ingestion`
- automatic preflight phase of scheduled sync execution

#### Expected stored data

- target snapshot selector
- resolved target snapshot ID
- baseline mode
- resolved baseline snapshot ID
- target snapshot info
- baseline snapshot info
- target snapshot metrics
- baseline snapshot metrics
- enabled models evaluated
- per-model validation status
- drift summary
- blocking reasons
- pass/fail outcome
- optional related ingestion

### 3. Read-Only Validation And Drift Query Layer

Add a shipped internal query layer for validation and drift analysis without turning the plugin into an NQE authoring system.

#### Rules

- NQE authoring stays in Forward.
- Operators may still use `query_id` or raw `query` in `Forward NQE Maps` for inventory sync.
- Validation queries are plugin-owned internal assets, not editable UI objects.

#### What the plugin should own

- shipped read-only validation NQEs
- shipped drift-comparison NQEs where needed
- strict expected output contracts for those internal queries

#### What the plugin should not own

- freeform authoring UX
- query templating UI
- transform DSL
- visual query composition

### 4. Sync And Ingestion UX Expansion

Expose validation and drift clearly in the existing workflow instead of hiding it in logs.

#### `Forward Sync` detail page

Add:

- linked drift policy
- latest validation run
- explicit `Run Validation` action
- explicit `Adhoc Ingestion` action that performs validation first
- clear last-known snapshot resolution summary

#### `Forward Validation Run` detail page

Show:

- target snapshot selector
- resolved target snapshot ID
- baseline mode
- resolved baseline snapshot ID
- target snapshot health
- baseline snapshot health
- enabled models
- per-model row counts and status
- blocking reasons
- whether ingestion was allowed
- link to related ingestion when one exists

#### `Forward Ingestion` detail page

Add:

- linked validation run
- preflight drift summary
- gating outcome
- force-merge status
- branch diff summary next to validation context
- explicit visibility into which NQE map executed for each enabled model

#### Merge UX

- normal merge remains manual
- normal merge is disabled when gating rules failed
- add an explicit `Force Merge` path for authorized users only
- record force-merge actor, timestamp, and reason

### 5. Ingestion Observability

Add enough metadata to explain a sync without reading logs or code.

#### Desired observability additions

- per-model executed map name
- per-model execution mode
  - `query_id`
  - raw `query`
- per-model query runtime
- per-model returned row count
- per-model failure count
- resolved target snapshot ID
- resolved baseline snapshot ID when applicable
- policy decisions and thresholds that were evaluated

#### Why

This makes it possible to answer:

- which map ran for this model
- which snapshot did it use
- why did validation fail
- why was merge blocked
- why did a branch contain unexpected deletions

### 6. Query Contract Enforcement

Strengthen the guarantee that built-in queries emit exactly the fields the plugin expects.

#### Current direction to preserve

- NetBox-ready output should come from NQE.
- Python should not normalize inventory values after the fact.

#### Additional hardening

- add explicit per-model output schemas for built-in sync queries
- add explicit per-query output schemas for internal validation queries
- fail tests when a required field is dropped or renamed
- fail tests when slug-safe or identity-safe assumptions drift
- preserve exact interface matching with no Python fallback

#### Concrete checks to add

- required output fields per built-in model
- valid slug-style output for all slug-bearing models
- exact interface name resolution behavior
- snapshot selection persistence
- no hidden `query_overrides`

### 7. CI And Local Validation Baseline

The project already has CI and local invoke tasks. The next phase should make them a more complete gate.

#### Existing baseline to preserve

- `pre-commit run --all-files`
- Python compile smoke
- `manage.py check`
- `manage.py test`
- docs build
- package build

#### Improvements to add

- include explicit query-contract tests in CI
- include release-asset build verification in CI
- keep test reruns fast and deterministic
- keep local validation and CI command sets aligned

#### Local command surface to preserve

- `invoke forward_netbox.lint`
- `invoke forward_netbox.check`
- `invoke forward_netbox.test`
- `invoke forward_netbox.docs`
- `invoke forward_netbox.package`
- `invoke forward_netbox.ci`
- `invoke forward_netbox.smoke-sync`

### 8. Release Automation

Reduce the amount of manual release surgery needed to keep a clean release story.

#### Goals

- deterministic wheel and sdist builds
- deterministic `SHA256SUMS`
- release tag aligned to the exact published tree
- release notes aligned to the actual commit
- less manual retagging and asset replacement work

#### Ideal end state

- one repeatable script or GitHub Action builds artifacts
- updates checksums
- publishes or refreshes the GitHub release
- verifies that `main`, the release tag, and the published assets match

PyPI can be the primary package distribution channel as long as GitHub release assets remain aligned to the exact published tree.

### 9. Live Validation And Smoke Workflow

Keep one lightweight live validation path outside normal CI because it depends on real Forward access.

#### Goals

- prove that the latest code still resolves a real source, network, and snapshot
- prove that built-in queries still execute in a real tenant
- prove that the branch-backed flow still reaches at least `ready_to_merge`

#### Existing foundation to keep

- `forward_smoke_sync` management command
- local `invoke forward_netbox.smoke-sync`

#### Improvements

- make smoke output easier to read
- record the specific models exercised
- optionally support a validation-only mode
- document the exact operational expectations for a successful smoke run

### 10. Parity Gap Review And Forward-Native Expansion

Do not let “future parity” stay vague.

#### First step

Publish a concrete parity matrix:

- supported now
- partially supported
- not yet implemented
- implemented but not yet live-validated

#### After the parity matrix

Choose Forward-native features that are worth building because they are useful, not because they mirror older systems.

Good candidates:

- snapshot comparison summaries
- merge blocking on destructive thresholds
- health regression detection
- validation-only dry runs
- per-model rerun or revalidate actions
- richer exposure of Forward snapshot health in the UI

## Proposed Delivery Phases

### Phase 1: Hardening And Visibility

- add query-contract tests
- add ingestion observability fields
- add parity matrix
- improve smoke-sync reporting
- improve docs around validation and limitations

### Phase 2: Validation Runs

- add `Forward Validation Run`
- add standalone validation action
- add automatic validation before ingestion
- persist snapshot comparison and per-model validation results

### Phase 3: Drift Policy And Merge Gating

- add `Forward Drift Policy`
- attach policy to syncs
- implement blocking rules
- add disabled merge and force-merge behavior

### Phase 4: Forward-Native Operator Experience

- add richer snapshot and drift summaries
- add per-model execution visibility
- add validation-only workflows and stronger troubleshooting guidance
- add targeted operator features that reduce investigation time

## Data Model Changes

If this roadmap is implemented, the main new persistent objects are:

- `ForwardDriftPolicy`
- `ForwardValidationRun`

Likely model extensions:

- `ForwardSync`
  - `drift_policy`
- `ForwardIngestion`
  - `validation_run`
  - gating outcome fields
  - force-merge metadata
  - richer per-model execution summaries

## API Changes

The likely new API surface includes:

- CRUD or read/write endpoints for `Forward Drift Policy`
- list/detail endpoints for `Forward Validation Run`
- sync actions for `run_validation`
- ingestion fields for:
  - `validation_run`
  - gating outcome
  - blocking reasons
  - force-merge metadata
  - per-model execution summaries

No new public NQE authoring API should be introduced.

## UI Navigation Changes

Likely navigation additions:

- `Drift Policies`
- `Validation Runs`

Likely page additions:

- validation run list/detail
- drift policy list/detail/edit

Likely page changes:

- sync detail page gains validation controls and policy visibility
- ingestion detail page gains validation and gating summaries

## Test Plan

### Unit And Contract Tests

- built-in query output schema tests
- validation query output schema tests
- slug validity tests
- exact interface resolution tests
- snapshot resolution and persistence tests
- no-hidden-override tests

### Model And Workflow Tests

- drift policy validation
- validation run creation without ingestion
- validation pass leading to ingestion creation
- validation fail blocking branch creation
- merge gating behavior
- force-merge permission and audit behavior

### Live Operational Tests

- real Forward smoke run against a known tenant
- target snapshot resolution using `latestProcessed`
- explicit snapshot selection path
- expected `ready_to_merge` workflow
- zero-issue or clearly explained issue outcomes

### Regression Tests

- current sync/branch/merge behavior still works when validation passes
- current built-in `NQE Maps` still execute normally
- current docs examples remain accurate

## Documentation Updates Needed When This Plan Starts

When implementation begins, update:

- top-level README
- user guide configuration docs
- usage and validation docs
- troubleshooting docs
- built-in NQE reference where relevant
- screenshots for any new validation or drift-policy pages

Also add:

- parity matrix
- known limitations section
- validation workflow guide
- force-merge explanation and safety caveats

## Risks And Design Checks

### Main risks

- adding too much policy complexity too early
- drifting into plugin-side query authoring
- reintroducing Python-side normalization
- making validation so strict that normal operator workflows become painful
- expanding UI surface faster than documentation and tests can keep up

### Design checks

- every new field should have a clear operator reason to exist
- every blocking rule should be explainable on the UI
- every internal validation query should have a tested schema contract
- every merge override should leave an audit trail
- every new capability should preserve the current simple install/release story

## Recommended Execution Order

1. Add parity matrix and docs hardening.
2. Add query-contract tests and observability fields.
3. Improve smoke-sync and live validation reporting.
4. Add `Forward Validation Run`.
5. Add `Forward Drift Policy`.
6. Add merge gating and force-merge audit path.
7. Add richer Forward-native drift summaries.

## Success Criteria

This roadmap is complete when:

- operators can run validation before ingestion
- snapshot comparison and health context are persisted and visible
- merge can be safely blocked based on policy
- force merge is explicit and audited
- built-in queries are protected by schema-contract tests
- live smoke validation remains easy to run
- docs fully explain both the standard workflow and the guarded workflow

## Assumptions

- NQE authoring remains a Forward responsibility.
- The plugin continues to support both `query_id` and raw `query` execution for sync maps.
- PyPI can be the primary package distribution mechanism, with GitHub Releases kept in sync as an alternate artifact channel.
- NetBox `4.5.x` remains the supported target until a deliberate compatibility expansion is planned.
