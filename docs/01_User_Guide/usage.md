# Usage and Validation

Run a sync from the `Forward Sync` detail page. The plugin executes the enabled
NetBox models through the configured NQE maps, records failures as `Forward
Ingestion Issues`, and stages the complete workload in one native NetBox
Branching branch for review and merge. Version 2.6 records runtime truth on the
sync, ingestion, branch, validation, issues, model results, jobs, and ownership
reconciliation rows; it does not create execution-ledger rows or offer a
direct-write backend.

## Self-Test Workflow

Use this flow to validate a new installation from the UI.

### 1. Create A Source

Open `Plugins > Forward > Sources > Add`.

Fill in:

- `Type`: `Forward SaaS` for `https://fwd.app`, or `Custom Forward deployment` for another URL
- `Username`
- `Password`
- `Network`

Important:

- `Network` will remain empty until the Forward account has both username and password configured.

Expected result:

- The form loads without errors.
- The `Network` field populates from the authenticated Forward tenant.
- Saving the source returns you to the source detail page.

![Forward Source](../images/forward-source.jpg)

### 2. Review Built-In NQE Maps

Open `Plugins > Forward > NQE Maps`.

Expected result:

- The seeded built-in maps are present.
- Each built-in map shows a `NetBox Model`, execution mode, and enabled state.
- Opening a built-in map displays either the shipped raw NQE text or the configured `Query ID`.

![Forward NQE Maps](../images/forward-nqe-maps.jpg)

### 3. Create A Sync

Open `Plugins > Forward > Syncs > Add`.

Recommended first pass:

- Select the source you just created.
- Optionally select a `Drift policy` if you want validation to block unsafe changes before any branch is created.
- Leave `Snapshot` at `latestProcessed`.
- Leave `Max changes per staging item` at `10000` unless measured workload
  density requires a different bound.
- Keep the default model selection enabled.
- Leave `Auto merge` enabled to merge the native branch automatically after staging.
- Disable `Auto merge` when you want to review the complete branch before queueing its merge.

See [Initial Baseline Strategy](configuration.md#initial-baseline-strategy) for the decision table.

Expected result:

- The sync saves cleanly.
- The sync detail page shows the selected source, network, snapshot selection, drift policy, latest validation result, and enabled model list.

![Forward Sync Detail](../images/forward-sync-detail.jpg)

### 4. Validate The Sync

From the sync detail page, click `Validate`.

Expected result:

- A `Forward Validation Run` is created without creating NetBox Branching branches.
- The validation run records the resolved snapshot, optional baseline snapshot, per-model query results, drift summary, and blocking reasons.
- If the selected drift policy blocks the run, the sync can be corrected before staging NetBox changes.
- If you intentionally accept the blocked result, open the validation run and use `Force allow` to record the override reason and reviewer before rerunning the sync.

![Forward Validation Detail](../images/forward-validation-detail.jpg)

### 5. Run An Adhoc Ingestion

From the sync detail page, click `Adhoc Ingestion`.

Expected result:

- A validation run is recorded before branch creation.
- A new `Forward Ingestion` is created.
- The sync creates one ingestion linked to one native NetBox Branching branch.
- The sync status progresses through branch-backed staging and then either queues merge or pauses for review.
- The ingestion records both the selected snapshot mode and the resolved snapshot ID used for NQE execution.
- The ingestion links to the validation run and persists per-model query execution results.
- If `Auto merge` is disabled, the sync pauses after the branch reaches `Ready to merge`.
- If any merge row fails, the branch returns to `Ready to merge`, the ingestion remains non-baseline, and retry applies the remaining changes.

### 6. Review The Ingestion

Open the ingestion detail page and inspect:

- status and timestamps
- snapshot selection and resolved snapshot ID
- snapshot state and processed time
- Forward snapshot metrics
- model results and validation status
- ingestion issues
- change diff when the ingestion used Branching
- branch linkage when the ingestion used Branching

Expected result:

- The ingestion detail page loads successfully.
- The ingestion shows the snapshot actually used for NQE execution.
- The ingestion shows Forward snapshot metrics for the selected snapshot when Forward returns them.
- The ingestion shows per-model execution mode, row count, delete count, runtime, and workload metadata when available.
- `Issues` is empty or contains actionable query/persistence errors.
- The change diff represents the staged NetBox changes for review.

![Forward Ingestions](../images/forward-ingestions.jpg)

![Forward Ingestion Detail](../images/forward-ingestion-detail.jpg)

### 7. Confirm The Merged Branch

With `Auto merge` enabled, the sync merges its native Branching branch after the
complete workload is staged. With `Auto merge` disabled, review the branch and
queue its merge from the ingestion. Do not start another sync while a prior
branch remains nonterminal.

Expected result:

- The Branching branch is marked merged.
- The synced objects are visible in standard NetBox object views.

## What To Check After A Successful Test

- Sites, devices, interfaces, prefixes, and the other selected models exist in NetBox.
- The latest ingestion has no unresolved issues.
- The latest ingestion shows the expected snapshot selector, resolved snapshot ID, and Forward metrics.
- The branch diff matches the expected object additions and updates.
- The source and sync statuses are back in a healthy state.

## CLI Smoke Validation

For a repeatable live smoke run outside GitHub Actions, configure a Forward
Source in NetBox and use the bundled management command through the local invoke
task. It automatically selects a reachable stored source without copying its
credential or printing private source identifiers:

```bash
invoke forward_netbox.smoke-sync --validate-only
```

Direct credential environment variables remain available only for bootstrapping
a source in an approved environment. Prefer the stored-source path for routine
validation.

Optional knobs:

- `FORWARD_SMOKE_URL` defaults to `https://fwd.app`
- `FORWARD_SMOKE_SNAPSHOT_ID` defaults to `latestProcessed`
- `FORWARD_SMOKE_MODELS` accepts a comma-separated list of enabled NetBox models
- `invoke forward_netbox.smoke-sync --validate-only` resolves the source/network/snapshot and executes the selected queries without creating an ingestion
- `--query-limit` limits rows fetched per query during `--validate-only`; normal syncs page through the full NQE result set
- `invoke forward_netbox.smoke-sync --plan-only --max-changes-per-staging-item 10000` builds the single-branch workload plan without creating a branch
- `invoke forward_netbox.smoke-sync --max-changes-per-staging-item 10000` sets the workload planning budget
- `invoke forward_netbox.smoke-sync --no-auto-merge` stages the one native Branching branch and pauses for review
- `python manage.py forward_smoke_sync --check-source` verifies stored-source selection and connectivity with redacted output
- `invoke forward_netbox.smoke-sync` uses the parity-tested bulk ORM model set by default. Pass `--enable-bulk-orm=False` only for adapter comparison evidence; models with relationship-specific semantics use their supported adapter path.

The normal UI/API `Run Sync` path uses one native Branching branch per sync. Use
the command-line smoke sync when you need plan-only output, a targeted model
subset, or a timed local validation run.

## Optional Module Import Readiness

The built-in `Forward Modules` map models chassis modules, line cards,
supervisors, fabric modules, and routing engines as native `dcim.module`
objects. NetBox modules require a matching bay. In 2.6, the sync creates a
missing `dcim.modulebay` in the same branch before it creates the module;
NetBox 4.6.5 with Branching 1.1.1 merges that MPTT dependency natively.

The **Module Readiness** page and CLI are read-only preflight tools. Use them
to inspect the exact bay creation plan before a large run:

```bash
python manage.py forward_module_readiness --sync-name "Forward Sync"
```

Local Docker shortcut:

```bash
invoke forward_netbox.module-readiness --sync-name "Forward Sync"
```

The command:

- the helper runs the module NQE map through the normal Forward API path
- it compares every `(device, module_bay)` result to existing NetBox module bays
- it writes `summary.json` with counts, missing device names, and the exact
  `(device, bay)` rows that the sync will create in its branch

Enable the `dcim.module` model and the `Forward Modules` NQE map for the sync.
SFPs and other optics remain in the inventory-item path by default. Matching
generic inventory rows for module-native component classes are removed during
inventory ingestion so the same hardware is not represented twice.

Expected result:

- the command creates or updates a disposable smoke `Source` and `Sync`
- the sync resolves the selected snapshot and runs a real ingestion
- the sync records validation and per-model execution metadata before branch staging
- the command exits non-zero if the sync fails or any ingestion issues are recorded
- in `--validate-only` mode, the command prints per-model query execution mode, row count, and runtime and exits non-zero on any query failure
