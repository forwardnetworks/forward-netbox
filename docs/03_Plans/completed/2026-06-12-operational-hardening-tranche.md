# Operational Hardening Tranche

## Goal

Implement the next production-hardening tranche around three contracts:

- field ownership and repeat-sync idempotence,
- parent dependency planning,
- query-ID drift remediation.

The target outcome is fewer silent misses, fewer unnecessary updates, and a
clear operator path when saved Forward query IDs drift away from the shipped
query sources.

## Constraints

- Keep NQE as the source of truth for row shape.
- Do not add retries or workaround loops for broken contracts.
- Preserve query-ID mode as the canonical path for Forward diffs.
- Keep customer data, network IDs, snapshot IDs, credentials, and screenshots
  out of committed artifacts.
- Keep behavior compatible with NetBox 4.5.9 and 4.6.1.
- Exclude async NQE and TurboBulk implementation from this tranche.

## Scope

1. Field ownership and idempotence
   - Add an explicit model-field contract for sparse rows where blank values are
     not authoritative clears.
   - Prove that repeat syncs do not clear or rewrite existing interface
     description, MTU, or speed when the row does not own those values.
2. Parent dependency planner
   - Add a contract that declares parent models required before child models.
   - Prove the branch apply dependency order honors that contract for every
     declared parent/child pair.
3. Query-ID drift remediation
   - Keep query-ID binding canonical.
   - Strengthen the diagnostic/remediation output so stale direct query IDs
     clearly point to the existing refresh action and live drift export.

## Touched Surfaces

- `forward_netbox/utilities/sync_contracts.py`
- `forward_netbox/utilities/sync_primitives.py`
- `forward_netbox/utilities/sync_interface.py`
- `forward_netbox/utilities/branch_budget.py`
- `forward_netbox/utilities/query_binding_resolution.py`
- `forward_netbox/utilities/health.py`
- `forward_netbox/utilities/execution_ledger_serialization.py`
- `forward_netbox/tests/test_sync.py`
- `forward_netbox/tests/test_query_binding.py`
- `forward_netbox/tests/test_health.py`
- `forward_netbox/tests/test_log_export.py`

## Approach

1. Add field ownership metadata to the sync contract.
   - Keep creates unchanged.
   - Filter only existing-object updates where a configured field receives a
     blank or null value that should not be treated as an authoritative clear.
2. Apply the first field ownership rule to `dcim.interface`.
   - Preserve existing description, MTU, and speed for sparse rows.
   - Continue applying explicit non-empty values.
   - Keep the LAG placeholder creation path safe for missing parents.
3. Add a parent dependency contract beside the branch apply order.
   - Declare expected parent models for child models.
   - Test that every declared parent is ranked before the child.
4. Strengthen query-ID drift remediation payloads.
   - Preserve query-ID mode as canonical.
   - Add a structured `refresh_query_ids` remediation action for direct query ID
     drift, not-found, and ambiguity cases.
   - Include the action counts in health and support-bundle diagnostics.

## Validation

- `rtk .venv/bin/invoke test-isolated --project-name forward-netbox-op1 --test-label forward_netbox.tests.test_sync.ForwardBranchBudgetPlanTest`
  - Passed; covers the parent-before-child apply dependency contract.
- `rtk .venv/bin/invoke test-isolated --project-name forward-netbox-op2 --test-label forward_netbox.tests.test_sync.ForwardSyncRunnerTest`
  - Passed; covers sparse interface field preservation and prior LAG
    idempotence regressions.
- `rtk .venv/bin/invoke test-isolated --project-name forward-netbox-op3 --test-label forward_netbox.tests.test_query_binding`
  - Passed; covers query-ID drift remediation payloads.
- `rtk .venv/bin/invoke test-isolated --project-name forward-netbox-op4 --test-label forward_netbox.tests.test_health.ForwardSyncHealthTest`
  - Passed; covers health summary remediation action codes.
- `rtk .venv/bin/invoke test-isolated --project-name forward-netbox-op5 --test-label forward_netbox.tests.test_log_export.ForwardIngestionLogExportViewTest`
  - Passed; covers support-bundle/live-diagnostic summary shape.
- `rtk .venv/bin/invoke test-isolated --project-name forward-netbox-op6 --test-label forward_netbox.tests.test_architecture_audit_command.ForwardArchitectureAuditCommandTest`
  - Passed; covers architecture-matrix visibility for field ownership.
- `rtk .venv/bin/invoke lint`
  - Passed.
- `rtk .venv/bin/invoke harness-check`
  - Passed.
- `rtk .venv/bin/invoke check`
  - Passed.

## Rollback

- Revert the field ownership contract additions and return affected adapters to
  direct upsert semantics.
- Remove the parent dependency contract test if it incorrectly models a real
  supported dependency.
- Restore prior query-ID remediation text if the stronger message causes UI or
  documentation regressions.

## Decision Log

- Chose a model-field ownership contract over one-off adapter conditionals so
  future sparse-row behavior is discoverable from the model contract.
- Limited the first ownership rule to interface description, MTU, and speed
  because that is the observed production churn class.
- Kept query-ID remediation on the existing Health page action instead of
  adding a second refresh path.
