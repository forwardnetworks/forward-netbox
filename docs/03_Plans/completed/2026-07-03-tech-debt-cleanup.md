# Tech-debt & hygiene cleanup (post-2.2.5)

**Date:** 2026-07-03

## Goal
With 2.2.5 shipped and no field reply pending action, clear accumulated repo
hygiene debt. A 4-agent read-only audit (dead code, hack scripts, doc/plan debt,
architecture) established that the **source is clean** — zero dead functions, zero
TODO/FIXME/HACK markers, no commented-out blocks, no pre-4.6/pre-1.1.0 version
shims, no leaked scratch files, no orphaned management commands. The real,
low-risk debt is documentation, plan-dir, git, and committed-evidence hygiene.
Genuine architectural items exist but are project-sized and are recorded as
tracked backlog rather than ripped out this pass.

## Constraints
- No behavior change, no schema/migration change, no dependency change.
- Do NOT remove load-bearing seams: the `_impl` + thin public-module pattern
  (`apply_engine`, `forward_api`, `query_fetch`, `query_binding`, `sync_routing`)
  is an intentional API boundary; collapsing it is churn across 30+ importers for
  no functional gain.
- Do NOT delete `ForwardApplyEngineChoices.TURBOBULK` / `PARQUET_BULK` — they are
  pinned by migration 0015 and are already inert (always in `rejected_engines`).
- Do NOT release this pass (deferred to the next turn).

## Touched Surfaces
- `README.md`, `docs/README.md`, `docs/01_User_Guide/README.md` — removed the
  self-referential duplicate `v2.1.3` and `v1.7.0` compat rows (6 rows total);
  regenerated `CHANGELOG.md` from the tables.
- `docs/03_Plans/active/ -> completed/` — archived 27 shipped release plans
  (versions ≤ 2.2.5).
- `docs/03_Plans/evidence/` — removed transient telemetry dumps (chaos / monitor /
  autorecover run JSON) and gitignored the transient patterns; kept the small
  audit fixtures referenced by docs/tests.
- Git: removed the stale `forward-netbox-1.3.3-merge-status-fix` worktree pinning
  local `main`; pruned local branches already merged into `origin/main`.
- `docs/03_Plans/active/2026-07-03-known-architectural-backlog.md` — new tracked
  backlog capturing the deferred structural items.

## Approach
Mechanical, reversible edits only. Duplicate rows removed by exact-match. Plans
archived via `git mv` (history preserved). Evidence pruned only for regenerable
telemetry categories after grepping for references. Branch prune limited to those
proven merged into `origin/main` (`git merge-base --is-ancestor`).

## Validation
Full Django suite (939) on 4.6.4 unchanged (no source touched); lint incl. the
`CHANGELOG matches README` gate; harness check (this plan satisfies the push
gate); sensitive-content gate. `mkdocs build --strict` for the doc edits.

## Rollback
Every change is `git revert`-able; archived plans and pruned evidence remain in
git history; deleted local branches are recoverable from `origin` or reflog.

## Decision Log
- Keep the intentional `_impl`/public-module seam and the inert TURBOBULK/
  PARQUET_BULK enum members — removing either is churn or a schema migration for
  zero functional benefit; the audit rated both "do not touch."
- Archive only unambiguous shipped **release** plans; the ~70 remaining active
  plans (architecture/feature/roadmap) need per-file triage and are left for a
  deliberate pass, not bulk-moved.
- Record the dual-apply-engine convergence, the unreachable `multi_branch` /
  density-budget machinery, and the churn identity-key fix as tracked backlog
  (project-sized, some blocked on field data) instead of executing risky refactors
  immediately before a release.

## Bundled changes
- Docs: removed duplicate compatibility-table rows; regenerated CHANGELOG.
- Repo hygiene: archived shipped release plans; pruned regenerable evidence dumps;
  removed a stale git worktree and merged local branches.
- Added a tracked known-architectural-backlog document. No code or behavior change.
