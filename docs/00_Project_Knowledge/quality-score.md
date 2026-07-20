# Quality Score

Last reviewed: 2026-07-18

Current score: **B+ release candidate**

This score reflects architecture and focused evidence in the 2.6 tree. It does
not replace the final release gate or customer-equivalent acceptance run.

## Strengths

- The supported matrix is exact: NetBox `4.6.5` and
  `netbox-branching` `1.1.1`.
- Every production sync uses one reviewable native Branching branch. There is no
  direct-write fast-bootstrap lane.
- The custom merge has an explicit incomplete state: failed rows produce issues,
  retain a ready branch, block baseline advancement, and remain retryable.
- Module bays created by device/module synchronization remain in the branch
  workflow, with merge coverage for parent-materialized bays.
- Durable main-schema ownership claims are per sync and ingestion generation.
  Exact-snapshot overlays, serialized union materialization, stale-worker
  rejection, conflict evidence, and deletion cleanup are covered by focused
  tests.
- Drift, health, audit, and support exports treat ownership finalization as a
  separate required condition after merge.
- CI and local gates cover sensitive-content scanning, pre-commit, docs, Django
  checks, migrations, focused scenarios, the full plugin suite, Playwright, and
  packaging.
- Shipped NQE maps and model contracts are versioned with the plugin.

## Operational Invariants

- The custom merge is not one database transaction across every branch row.
  Strict incomplete-merge state and idempotent retry are therefore critical and
  must remain release-gated.
- Large inventories can expose different branch ObjectChange counts than NQE
  row counts because one row can create or update related NetBox objects. Scale
  evidence must measure branch and merge facts, not infer them from fetched rows.
- Local docs checks require the Poetry development dependencies or the
  dependency set used by CI.
- Synthetic fixtures cannot prove a customer's query publication state, tag
  policy, or exact Forward data. Customer-equivalent acceptance is release
  evidence, not a substitute for repository tests.

These are release-test invariants, not outstanding implementation work. A new
reproducible correctness failure changes the release status immediately and is
fixed with regression evidence before release.

## Release Bar

The 2.6 release candidate is acceptable only when all of the following describe
the same commit and exact runtime matrix:

- migrations apply cleanly and `makemigrations --check` reports no drift
- focused merge, ownership, scope, parent, module, drift, health, and support
  regressions pass
- `invoke harness-check`, `invoke harness-test`, `invoke lint`, `invoke check`,
  `invoke scenario-test`, `invoke test`, `invoke docs`, and `invoke ci` pass
- Playwright and package installation tests pass against NetBox `4.6.5` with
  Branching `1.1.1`
- `forward_ownership_audit --fail-on-inconsistent` passes after upgraded syncs
  establish current ownership generations
- no incomplete branch merge, pending/failed ownership domain, parent conflict,
  or branch schema pending migrations is presented as convergence
- customer-equivalent acceptance confirms the reported scope, endpoint,
  virtual-parent, DLM, CVE, software-version, and drift workflows

Passing an earlier commit or a different dependency matrix is not release
evidence for 2.6.

## Maintainer Priorities

- Preserve tests at code boundaries before moving adapter or orchestration code.
- Keep one execution path and one persisted runtime state model.
- Treat support-bundle redaction and aggregate-only ownership diagnostics as
  security requirements.
- Keep release evidence reproducible and tied to commit, image, dependency
  versions, and test command.
- Keep active plans limited to work with an owner, current decision, and
  executable validation. This document intentionally makes no assertion about
  the number of files in the active-plan directory.
