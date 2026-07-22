# Remove the dead density-learning write loop

**Date:** 2026-07-04

## Goal
Delete the density-learning *write* loop, which the 2026-07-04 architecture
investigation confirmed is dead: `density_learning.update_density_learning` and
`should_accept_observation` have no production caller (only each other and two
tests). The learned density profile is never updated from observed runs, so this
code has run for zero effect.

## Constraints
- Remove ONLY the dead write loop. The rest of `density_learning.py` is live and
  stays: `density_profile_summary`, `normalize_density_map`,
  `normalize_density_profile`, `density_budget_policy` (imported by
  `branch_budget`, `sync_state`, `execution_telemetry`, `health_summary_blocks`).
- Keep the shared helpers `clamp_density`, `_safe_int`, `_safe_float` (used by the
  live functions) and the `math`/`timezone`/`parse_datetime` imports (still used).
- `max_changes_per_branch` and the budget READ path are untouched.
- No schema/behavior change; no release this pass.

## Touched Surfaces
- `forward_netbox/utilities/density_learning.py` — delete `update_density_learning`
  and `should_accept_observation`, plus the three constants only they used
  (`DENSITY_UPDATE_ALPHA`, `DENSITY_OUTLIER_Z_THRESHOLD`,
  `DENSITY_OUTLIER_RATIO_THRESHOLD`).
- `forward_netbox/tests/test_sync.py` — remove the import and the two tests that
  exercised only the dead loop (`test_density_learning_rejects_large_outlier_after_warmup`,
  `test_density_learning_accepts_warmup_samples`).

## Approach
Confirm isolation first (grep: no external importer of the two functions or the
three constants; shared helpers used ≥6× outside the dead functions), then delete
by function boundary and drop the orphaned constants and their two tests.

## Validation
Full Django suite on 4.6.4: 937 pass (was 939; the two removed tests only covered
the dead loop), 28 skip. Lint (flake8 confirms no now-unused imports) + harness.

## Rollback
Revert the commit; code-only, no schema/migration, so a downgrade is clean.

## Decision Log
- Scope to the write loop only: the density budget READ/telemetry path is live and
  a broader density simplification (fixed sub-batch size vs. the profile machinery)
  is a separate, larger question left for a future pass.
- Keep this out of the `multi_branch` removal commit to stay single-purpose, per
  the investigation's recommendation.

## Bundled changes
- Removed the dead density-learning write loop (2 functions + 3 constants) and its
  2 tests. No behavior change; suite green.
