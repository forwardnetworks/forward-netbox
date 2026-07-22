# 2026-05-23 Compatibility Cache Retirement Completion

## Goal

Complete runtime retirement of compatibility `_branch_run` orchestration writes
so execution-ledger state is the single active control plane.

## Constraints

- Preserve read-through compatibility for upgrade payloads.
- Keep Branching lifecycle behavior unchanged for active ledger-backed runs.
- Keep stale compatibility cache cleanup available through native tooling.

## Touched Surfaces

- `forward_netbox/utilities/multi_branch_lifecycle.py`
- `forward_netbox/utilities/resumable_branching.py`
- `forward_netbox/jobs.py`
- `forward_netbox/management/commands/forward_prune_compatibility_cache.py`
- `forward_netbox/tests/test_jobs.py`
- `forward_netbox/tests/test_sync_state.py`
- `forward_netbox/tests/test_ingestion_merge.py`
- `forward_netbox/tests/test_log_export.py`
- `forward_netbox/tests/test_prune_compatibility_cache_command.py`
- `docs/03_Plans/active/2026-05-23-long-term-architecture-roadmap.md`

## Approach

1. Remove active runtime fallback writes to compatibility sync parameters.
2. Ensure legacy `_branch_run` continuation upgrades into execution ledger
   before any stage queueing/execution.
3. Keep operational prune/report command for stale payloads.
4. Validate with compatibility-focused regression tests and harness/check gates.

## Implementation

- Runtime phase state (`set_runtime_phase`) now updates execution ledger only and
  no longer mutates compatibility payloads.
- Legacy no-run plan-item mutation path now returns no-op instead of writing
  compatibility state.
- Stage queueing now requires an execution run and upgrades legacy
  compatibility state to ledger before queueing when needed.
- Stage job startup now attempts upgrade-to-ledger when a legacy state exists
  and no execution run row exists.
- Stale compatibility prune command remains available and tested.

## Validation

- `docker compose --project-name forward-netbox --project-directory development exec -T netbox /opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py test --keepdb --noinput forward_netbox.tests.test_jobs forward_netbox.tests.test_sync_state.ForwardSyncStateHelperTest forward_netbox.tests.test_ingestion_merge forward_netbox.tests.test_log_export forward_netbox.tests.test_prune_compatibility_cache_command`
- `poetry run invoke harness-check`
- `poetry run invoke check`

## Rollback

- Restore compatibility fallback writes in lifecycle/resumable paths.
- Revert stage-job/queue upgrade-to-ledger behavior.
- Keep prune tooling intact for stale payload cleanup.

## Decision Log

- Compatibility payload support remains read-through for upgrade continuity, but
  active orchestration writes are retired to prevent split-brain runtime state.
- Upgrade-first queueing preserves backward compatibility without carrying
  compatibility writes forward as an active runtime path.
