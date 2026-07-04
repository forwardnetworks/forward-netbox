# Remove the dead `multi_branch` fossil

**Date:** 2026-07-04

## Goal
Retire the always-True `multi_branch` scaffolding left over from the pre-2.0
branch-per-shard executor. Since 2.0, single-branch is the only execution path, so
`multi_branch` is a fossil written to every sync's parameters and surfaced in the
workload summary/display for no behavioral reason.

## Constraints
- Prove unreachability before removing. (Done: an investigation workflow with an
  adversarial reachability pass concluded, high confidence, that multi-branch
  execution is unreachable — the only dispatch builds `ForwardSingleBranchExecutor`
  unconditionally and exactly one branch is provisioned.)
- BACK-COMPAT (hard constraint): old `ForwardSync` rows have `multi_branch` in
  their stored JSON `parameters`. `clean_forward_sync` rejects unknown keys, so
  `multi_branch` MUST stay in the allowlist. The change is "stop writing", not
  "reject".
- Do NOT touch `max_changes_per_branch` — it is a live telemetry/budget param.
- No schema migration (the keys live only in the `parameters` JSONField).
- No release this pass.

## Touched Surfaces
- `forward_netbox/utilities/model_validation.py` — drop the
  `parameters["multi_branch"] = True` write; KEEP the allowlist entry (line ~331)
  and all `max_changes_per_branch` handling.
- `forward_netbox/utilities/sync_facade.py` — drop the `multi_branch` write in
  `normalize_forward_sync`; delete the `uses_multi_branch()` definition.
- `forward_netbox/forms.py` — drop the two `"multi_branch": True` literals in
  `ForwardSyncForm.clean()`/`save()`.
- `forward_netbox/utilities/sync_state.py` — drop the display-dict write and the
  `uses_multi_branch` workload-summary key.
- `forward_netbox/models.py` — delete the `uses_multi_branch()` method and its
  `sync_facade` import.
- `forward_netbox/jobs.py` — fix a stale "multi-branch plan" docstring.
- `tasks.py` — delete the dead `ForwardMultiBranchExecutorAdaptiveSplitTest`
  invoke target (the class no longer exists).
- Tests: trim `multi_branch`/`uses_multi_branch` assertions in `test_models.py`,
  `test_sync_facade.py`, `test_issue_rendering.py`; keep the live
  `max_changes_per_branch` assertions and the legacy `multi_branch` input keys as
  back-compat coverage.

## Approach
Leaf writers first, definition last: stop the writes, remove the display/summary
consumers, then delete the `uses_multi_branch` supplier chain (method → import →
definition). Update the tests that asserted the removed writes, keeping the live
budget assertions. `multi_branch_lifecycle.py` and `ForwardFastBootstrapExecutor`
keep their (misleading) names — a rename is a separate cosmetic follow-up.

## Validation
Full Django suite on 4.6.4 (939 pass, 28 skip — unchanged). Explicit back-compat
proof: a `ForwardSync` whose stored parameters contain `multi_branch=True` still
passes `clean()`, retains the key, and keeps `max_changes_per_branch` live.
Lint / harness / sensitive.

## Rollback
Revert the commit; the change is code-only (no schema/migration), so a downgrade is
clean. Old rows are unaffected either way (the key was and remains allowlisted).

## Decision Log
- "Stop writing" not "strip": stripping the key from existing rows would churn
  every row and can race in-flight syncs for zero benefit — the key is inert.
- Keep `max_changes_per_branch` and the density-learning read/budget path (live);
  only the density-learning *write* loop is separately dead and tracked as a
  distinct follow-up, deliberately out of this change.
- Keep the two allowlist entries so old syncs validate; this is the one hard
  constraint the whole change is shaped around.

## Bundled changes
- Removed the dead `multi_branch` fossil (writes + `uses_multi_branch` +
  workload-summary key). No behavior change; back-compat preserved; suite green.
