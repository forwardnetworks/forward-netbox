# Branching Prune Delete Architecture

## Goal

Make tag-scope prune runs durable for large filtered syncs by aligning Branching execution with the existing dependency-aware sync contract: apply rows first, then delete out-of-scope rows in child-to-parent model order.

## Constraints

- NQE remains the source of truth for row shape and normalization.
- NetBox Branching remains the staging and merge surface.
- Device tag prune must not delete parent devices before NetBox child objects that protect them.
- Resumable Branching must be able to retry the exact same shard after a worker timeout, split, or merge handoff.
- Existing execution ledgers created before this change must remain readable.
- Protected dependency misses during prune are row skips with diagnostics, not shard-fatal failures.

## Touched Surfaces

- `forward_netbox/utilities/branch_budget.py`
- `forward_netbox/utilities/multi_branch_executor.py`
- `forward_netbox/utilities/multi_branch_lifecycle.py`
- `forward_netbox/utilities/resumable_branching.py`
- `forward_netbox/utilities/execution_ledger*.py`
- `forward_netbox/utilities/sync_reporting.py`
- `forward_netbox/utilities/sync_routing_impl.py`
- `forward_netbox/models.py`
- `forward_netbox/migrations/0020_execution_step_operation.py`
- `forward_netbox/tests/test_sync.py`

## Approach

1. Split Branching workloads into explicit `apply` and `delete` plan operations.
2. Preserve normal apply order for upserts.
3. Sort delete work with a model dependency order so protected children such as cables, IP addresses, interfaces, routing objects, inventory, and modules are pruned before parent devices.
4. Persist the plan operation on execution steps and JSON plan items so resumable Branching can reselect the same operation after a retry.
5. Treat `mixed` operation from older ledgers as a wildcard for backward compatibility.
6. Count `ForwardDependencySkipError` during deletes as skipped rows and log them as warnings, while still recording issues for support export.
7. Remove BGP helper objects that are owned by the imported peer row when they are no longer referenced, so device deletion is not blocked by orphaned `BGPRouter`/`BGPScope` objects.

## Validation

- Focused Django tests for Branching plan construction, adaptive split resume, and row issue accounting.
- `ruff` over touched Python modules.
- `makemigrations --check --dry-run` inside the NetBox container.
- `migrate forward_netbox` inside the local NetBox container.
- `harness-check`.
- Full `invoke ci` before any release.

## Rollback

Revert the code and migration together. If a test environment has applied `0020_execution_step_operation`, roll back with `python manage.py migrate forward_netbox 0019` before downgrading the plugin code.

## Decision Log

- Rejected: only increase `dcim.device` delete density again. That reduces branch size but does not fix parent-first prune ordering or protected child failures.
- Rejected: ignore protected dependency errors silently. Operators need diagnostics, but the row should be a skip when the prune path cannot delete because an external or not-yet-pruned child still protects it.
- Rejected: use labels to infer delete shards. Resumable execution needs a first-class operation field, not display text parsing.
