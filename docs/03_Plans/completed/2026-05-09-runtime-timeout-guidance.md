# Runtime Timeout Guidance

## Goal

Make large-sync runtime limits visible before operators wait through a long run
that is likely to be killed by NetBox worker timeout settings.

## Constraints

- Do not change sync execution behavior or block existing runs.
- Keep Branching and fast bootstrap semantics unchanged.
- Avoid migrations or new dependencies.
- Do not cut a release for this tranche.

## Touched Surfaces

- `forward_netbox/utilities/runtime_guidance.py`
- `forward_netbox/utilities/sync_orchestration.py`
- `forward_netbox/utilities/multi_branch_executor.py`
- `forward_netbox/tests/test_sync_orchestration.py`
- `forward_netbox/tests/test_sync.py`
- `docs/01_User_Guide/configuration.md`
- `docs/01_User_Guide/troubleshooting.md`

## Approach

- Inspect NetBox `RQ_DEFAULT_TIMEOUT` without requiring it to be configured.
- Warn when the worker timeout is lower than the Forward source timeout.
- Warn when a large Branching plan is built under a short worker timeout.
- Keep warnings in normal sync logs so operators see them in the existing UI.
- Document the expected timeout, branch-budget, and fast-bootstrap sizing
  guidance for large baselines.
- Verify the current Forward public NQE API timeout path before changing the
  plugin default timeout.

## Scope

- Add non-blocking sync warnings when NetBox `RQ_DEFAULT_TIMEOUT` is lower than
  the Forward source timeout.
- Add a large Branching plan warning when a sharded run is planned with a short
  worker timeout.
- Document how to size Forward source timeout, NetBox worker timeout, Branching
  branch budget, and fast bootstrap usage.

## Validation

- `python manage.py test forward_netbox.tests.test_sync_orchestration.ForwardSyncOrchestrationHelperTest.test_run_forward_sync_warns_when_worker_timeout_is_lower_than_source_timeout --keepdb --noinput`
- `python manage.py test forward_netbox.tests.test_sync.ForwardMultiBranchExecutorAdaptiveSplitTest.test_load_execution_context_warns_for_large_plan_with_short_worker_timeout --keepdb --noinput`
- `invoke docs`

## Rollback

Remove the runtime guidance helper calls and the documentation additions. No
database state or migration cleanup is required.

## Decision Log

- Rejected making low `RQ_DEFAULT_TIMEOUT` a hard preflight failure because small
  syncs can still complete successfully and operators may intentionally run with
  short worker limits.
- Rejected adding a new plugin setting because NetBox already owns worker
  timeout policy through `RQ_DEFAULT_TIMEOUT`.
- Rejected lowering the Forward source timeout to 300 seconds because the
  current Forward source shows public `/nqe` using a 20-minute org-configured
  compute timeout by default, with a longer HTTP deferred-response wrapper.

## Notes

This does not change execution behavior and does not cut a release. It prepares
the next release with clearer operational guidance for large baselines.
