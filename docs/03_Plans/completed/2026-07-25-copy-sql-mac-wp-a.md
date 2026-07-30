# COPY/SQL Apply Engine WP-A: MAC Address

## Goal

Add a disabled-by-default, branch-only `copy_sql` apply engine and prove the
complete `dcim.macaddress` create, update, no-op, approved-delete, identity,
native evidence, rollback/fallback, and merge path against the existing engine.

## Constraints

- Work only on `feat/copy-sql-apply-engine` in `/tmp/forward-netbox-copysql`.
- Keep NetBox Branching and the one-provisioned-branch execution model.
- Support only the exact NetBox 4.6.5, Branching 1.1.1, and pinned optional-plugin
  distribution tuple; fail closed for any other runtime.
- Default `copy_sql` off, require a per-sync enable flag, keep a per-model kill
  switch, and retain the existing bulk/adapter engines as fallback.
- Consume only the planner's `upsert_rows` and approved `delete_rows`; never infer
  a delete from staging-table absence.
- Commit MAC target changes, FK-driven side effects, ObjectChanges, and
  ChangeDiffs in one PostgreSQL transaction on the branch connection.
- Preserve row validation, dependency skip/failure, canonical MAC coalesce,
  ambiguity, primary-MAC reassignment protection, statistics/issues, and job
  timeout identity.
- Make no version bump, release change, GitHub mutation, production mutation, or
  commit.

## Touched Surfaces

- `forward_netbox/choices.py`
- `forward_netbox/utilities/apply_engine.py`
- `forward_netbox/utilities/apply_engine_decision.py`
- new COPY/SQL model-spec and MAC execution modules under
  `forward_netbox/utilities/`
- `forward_netbox/utilities/branch_lifecycle.py`
- sync parameter normalization/validation and the ForwardSync form/API surfaces
- focused apply-engine and paired-branch integration tests
- user/reference configuration documentation
- an isolated realistic-MAC benchmark harness and recorded local evidence

## Approach

1. Add a model-spec registry whose only initial member is `dcim.macaddress`.
   Gate selection on an active provisioned branch, the global opt-in flag, the
   model allowlist, the model kill-switch list, the exact supported runtime tuple,
   and absence of unsupported runtime hooks.
2. Add a combined plan-item apply boundary so `copy_sql` receives approved
   upserts and deletes together. Existing engines retain their current dispatch.
3. COPY raw planner rows and stable row ordinals into transaction-scoped temporary
   tables. Resolve device/interface dependencies, canonical MAC identity,
   duplicate/ambiguous buckets, primary-MAC protection, and insert/update/no-op/
   explicit-delete sets in SQL. Route ineligible identity buckets to the current
   engine without splitting a target identity.
4. Apply eligible branch target mutations and FK side effects with set DML.
   Project exact MAC full/clean snapshots, insert native branch ObjectChanges,
   and update/insert main-schema ChangeDiffs on the same branch connection and
   transaction, with a branch/model advisory lock.
5. On an unexpected SQL fault, leave the atomic block, assert target and evidence
   fingerprints are unchanged, then replay the complete item through the current
   engine. Re-raise `JobTimeoutException` without fallback.
6. Differentially compare paired branches for target/side-effect state,
   statistics/issues, complete ObjectChange payloads, ChangeDiff state, rollback,
   and merge behavior. Keep unsupported tuples and killed models on the current
   engine.
7. Benchmark a deterministic realistic MAC operation mix in a distinct isolated
   Compose project for at least three rounds per engine, reporting wall time,
   statements per operation, peak RSS, and variance.

## Validation

- Focused unit tests for selection gates, version tuples, model kill switches,
  canonical/ambiguous identity, and current-engine fallback.
- Paired real-branch integration tests for create, update, no-op, delete,
  identity change, ambiguity, missing dependencies, invalid rows, complete
  ObjectChange/ChangeDiff equality, side effects, and final merge state.
- Fault injection after target DML, after ObjectChange insert, and during
  ChangeDiff mutation; each must prove rollback before clean fallback.
- `invoke harness-check`
- focused isolated Django tests on NetBox 4.6.5 / Branching 1.1.1
- `invoke harness-test`, `invoke lint`, `invoke check`, `invoke scenario-test`,
  and `invoke test` as time and repository state permit; record any unrun gate.
- At least three accepted current-versus-COPY/SQL MAC benchmark rounds in a
  unique Compose project, with correctness checks before accepting each result.

## Rollback

Leave the global flag disabled or add `dcim.macaddress` to the model kill-switch
list. Both routes select the existing engine without changing planned rows or
branch lifecycle. Code rollback removes the registry/engine and combined dispatch;
no migration or persisted target state is introduced. A failed SQL attempt must
be transactionally empty before fallback, so it requires no cleanup.

## Decision Log

- The SQL path owns upserts and approved deletes together because separate engine
  calls cannot prove one target/evidence transaction or safely suppress a
  same-identity approved-delete conflict.
- Direct target DML is allowed only for the exact MAC spec. Generic serializer or
  model inference is rejected because ObjectChange JSON is executable merge state.
- Duplicate/ambiguous identities and primary-MAC reassignments are whole-bucket
  fallback conditions. Choosing an arbitrary target or bypassing `MACAddress.clean()`
  would weaken current correctness.
- Optional plugins remain supported by the existing engines. Unknown versions or
  runtime hooks disable COPY/SQL instead of attempting best-effort SQL.
- The exact MAC model has no model/unique constraints on the allowlisted tuple.
  SQL resolves dependency existence and primary-MAC protection; the native
  `MACAddressField` validates each actual create/update candidate. Any applicable
  CustomField, custom validator, protection rule, EventRule, or Branching field
  migrator routes the item to the existing engine.
- Identity/dependency resolution uses materialized set aggregates. A rejected
  lateral-lookup draft was quadratic at 10,000 rows and was replaced before the
  accepted benchmark evidence was collected.

## Execution Evidence

Runtime: NetBox 4.6.5, `netboxlabs-netbox-branching` 1.1.1, PostgreSQL in the
isolated Compose project `fnb-copysql-wpa-20260725`. Branch provisioning and
fixture construction were excluded from timing.

The paired-branch suite compares complete normalized target state, Bookmark,
JournalEntry, Subscription, and TaggedItem side tables, issues/statistics, every
ObjectChange field and JSON payload, every ChangeDiff field, rollback/fallback,
and the final merge result. Its mixed case covers create, update, no-op, explicit
delete, formatted coalesce identity, identity replacement, ambiguous identity,
missing device/interface, invalid MAC, primary-MAC reassignment, relation-bound
delete, absent approved delete, and a three-way ChangeDiff conflict.

Fault injection after target DML, after ObjectChange insertion, and during
ChangeDiff update proves a clean rollback fingerprint before the full item is
replayed through the current engine. The resulting branch state exactly matches
the current-engine reference branch.

Accepted benchmark fixture: 10,000 planner rows per cell using the realistic-mix
campaign's synthetic steady ratios: 9,000 no-op, 700 update, 200 create, and 100
approved delete. Each of six cells verified 9,900 final target rows and exactly
200 create, 700 update, and 100 delete ObjectChanges/ChangeDiffs.

| Engine | Wall seconds, mean +/- sample SD (range) | Observed statements/row | Peak RSS MiB, mean +/- sample SD | Incremental peak RSS MiB |
| --- | ---: | ---: | ---: | ---: |
| current `bulk_orm` | 9.027 +/- 0.096 (8.919-9.102) | 0.7144 +/- 0 | 326.34 +/- 0.03 | 132.07 +/- 0.51 |
| `copy_sql` | 1.005 +/- 0.014 (0.991-1.020) | 0.0049 +/- 0 | 208.65 +/- 0.64 | 14.08 +/- 1.00 |

Realized comparison: **8.98x wall-clock speedup**, **145.80x fewer observed
database round trips**, and COPY/SQL peak RSS at **63.94%** of current. Query
counting uses Django execute wrappers on both aliases and explicitly counts the
single psycopg COPY operation. These are MAC-only synthetic fixture results, not
an extrapolation to the supplied live 1,126,847-row run.

Machine-readable evidence is under
`docs/03_Plans/completed/evidence/copy-sql-mac-wp-a/`:

- `mac-bulk-round-{1,2,3}.json`
- `mac-copy-round-{1,2,3}.json`
- `mac-realistic-mix-summary.json`

## Not Yet Proven

- No model other than `dcim.macaddress` has a COPY/SQL model contract.
- No claim is made for a runtime other than the exact allowlisted NetBox,
  Branching, plugin-app, and optional-distribution tuple.
- Rows affected by CustomFields, dynamic validators/protection rules, EventRules,
  Branching field migrators, primary-MAC protection, or relation-side deletes
  deliberately use the current engine; raw SQL performance is not proved for
  those buckets.
- The benchmark is a deterministic 10,000-row synthetic steady mix with three
  rounds per engine. It is not a live Forward dataset, a million-row scale test,
  a concurrent-sync/deadlock campaign, or a merge-performance benchmark.
- The known fixture/live per-row cost gap means neither the 8.98x result nor the
  statement ratio should be projected directly onto the live run.

## Validation Results

- Focused Django suite: 23 tests passed. This includes the complete COPY/SQL
  file, configuration/model/form defaults, current bulk-MAC engine tests, and
  adapter-vs-bulk MAC parity tests.
- `invoke harness-check`: passed.
- `invoke harness-test`: 205 tests passed.
- Direct isolated-container `manage.py check`: no issues.
- `invoke docs`: passed.
- Targeted reorder-python-imports, Black, Flake8, whitespace, EOF, and sensitive
  content hooks: passed.
- `invoke check` itself was not usable with the required distinct Compose project
  name; its wrapper reported the default `netbox` service absent. The underlying
  Django check passed in `fnb-copysql-wpa-20260725-netbox-1` as recorded above.
- No version bump, release gate, full release suite, commit, GitHub action, or
  production action was performed.
