# Next Production Hardening Tranche

## Goal

Move the plugin toward production-complete operation without relying on NQE async
or TurboBulk by implementing the next hardening tranche in this order:

1. Query governance CI/live drift gate.
2. Full model field ownership matrix.
3. Support bundle diagnosis summary.
4. Dependency planner dry-run.
5. Optional plugin adapter framework.
6. Carefully scoped ACI/CIMC native mappings.

## Constraints

- Keep Forward NQE and NetBox adapter contracts as the source of truth.
- Do not persist raw customer rows in support bundles or durable orchestration
  state.
- Prefer query ID and repository-path governance over raw-query drift.
- Keep optional plugin support capability-gated.
- Avoid retries or workaround behavior where an explicit contract can prevent
  the failure.

## Touched Surfaces

- `forward_netbox/utilities/query_binding_resolution.py` for validation-org
  query drift gate status, remediation classification, and missing-source gaps.
- `forward_netbox/utilities/sync_contracts.py` and
  `forward_netbox/utilities/model_contracts.py` for model field ownership and
  preserve-on-blank audit output.
- `forward_netbox/utilities/execution_ledger_serialization.py` for support
  bundle diagnosis summary generation.
- `forward_netbox/utilities/branch_budget.py` and
  `forward_netbox/utilities/health_summary_blocks.py` for dependency dry-run
  planning and health surfacing.
- `forward_netbox/utilities/plugin_integrations/registry.py`,
  `forward_netbox/utilities/query_registry.py`, and architecture audit command
  output for optional plugin adapter/query contracts and ACI native mapping
  governance.
- `forward_netbox/queries/forward_aci_apic_cimc_inventory.nqe`,
  query registry fixtures, and reference docs for the scoped APIC CIMC native
  inventory map.
- Focused tests under `forward_netbox/tests/` for query governance, query
  registry, architecture audit, sync field ownership, health dependency dry-run,
  and support-bundle diagnosis behavior.

## Decision Log

- Treat validation-org missing source as an unproved gate, not as a soft pass,
  because query-ID mode only stays safe when the saved source can be compared to
  the bundled local query.
- Keep preserve-on-blank as explicit per-model ownership metadata so sparse
  Forward rows cannot silently clear locally meaningful NetBox fields.
- Keep support-bundle diagnosis derived and sanitized; it may include status,
  counts, model names, query IDs, and action hints, but not raw customer rows.
- Make the dependency dry-run read-only and contract-based so it prevents
  missing-parent failures before branch staging instead of retrying after row
  errors.
- Keep optional plugin integration adapter contracts registry-driven, while
  keeping plugin installation optional and capability-gated.
- Bind APIC CIMC inventory to the ACI integration for governance but to native
  `dcim.inventoryitem` for the NetBox target model; do not treat it as a
  `netbox-cisco-aci` plugin model.

## Approach

### 1. Query Governance Gate

- Harden validation-org query audit so a successful gate proves local bundled
  compiled NQE source matches the live validation org folder.
- Treat missing live source as an unproven gate condition, not a pass.
- Expose structured action/status fields so CI, release readiness, Health, and
  support bundles can point at the exact remediation.

### 2. Field Ownership Matrix

- Promote preserve-on-blank behavior from the first interface-specific contract
  to an explicit per-model field ownership matrix.
- Export the matrix through the architecture audit so each model has visible
  ownership semantics.
- Add tests that prevent sparse Forward rows from clearing locally meaningful
  NetBox fields.

### 3. Support Bundle Diagnosis Summary

- Add a concise diagnosis block to support bundles that summarizes likely cause,
  required next action, and proof fields without raw row data.
- Include query drift, dependency planner, recovery, freshness, and model issue
  signals.

### 4. Dependency Planner Dry Run

- Add a dry-run dependency planning surface that reports enabled models, missing
  parent-model prerequisites, apply-order rank, and expected blocked child
  surfaces before staging branches.

### 5. Optional Plugin Adapter Framework

- Keep the current optional plugin integrations but move adapter metadata toward
  a registry-driven contract that can support more plugins without scattered
  conditionals.

### 6. Scoped ACI/CIMC Native Mappings

- Keep ACI/CIMC scope bounded to native NetBox representations that already have
  identity proof and repeat-sync tests.
- Do not add tenants, VRFs, BDs, EPGs, contracts, L3Outs, or bindings without
  separate parser identity proof and repeat-sync validation.

## Validation Plan

- Focused tests for query governance, model contracts, support bundles, branch
  planning, optional plugin integrations, and CIMC mapping.
- `invoke harness-check`
- `invoke lint`
- `invoke check`
- Full isolated Django test run when local shared runtime is busy.

## Validation Evidence

- `rtk .venv/bin/invoke test-isolated --project-name forward-netbox-next-query --test-label forward_netbox.tests.test_query_registry.QueryRegistryTest`
  passed: 51 tests.
- `rtk .venv/bin/invoke test-isolated --project-name forward-netbox-next-arch --test-label forward_netbox.tests.test_architecture_audit_command.ForwardArchitectureAuditCommandTest`
  passed: 11 tests.
- `rtk .venv/bin/invoke test-isolated --project-name forward-netbox-next-sync --test-label forward_netbox.tests.test_sync.ForwardSyncRunnerTest`
  passed: 225 tests.
- `rtk .venv/bin/invoke test-isolated --project-name forward-netbox-next-health --test-label forward_netbox.tests.test_health.ForwardSyncHealthTest`
  passed: 30 tests.
- `rtk .venv/bin/invoke test-isolated --project-name forward-netbox-next-log --test-label forward_netbox.tests.test_log_export.ForwardIngestionLogExportViewTest`
  passed: 18 tests.
- `rtk .venv/bin/invoke test-isolated --project-name forward-netbox-next-validation --test-label forward_netbox.tests.test_validation_org_query_audit_command.ValidationOrgQueryAuditTest`
  passed: 5 tests.
- `rtk .venv/bin/invoke harness-check` passed.
- `rtk .venv/bin/invoke lint` passed after formatting hooks updated imports and
  Black formatting.
- `rtk .venv/bin/invoke check` passed with no system check issues.
- `rtk git diff --check` passed.

## Rollback

Each item should be independently reversible:

- Query gate: remove stricter source-proof classification and return validation
  org audit to the prior warning-only behavior.
- Field ownership: remove model field ownership entries and central filtering.
- Support summary: remove only the derived diagnosis block.
- Dependency dry-run: remove read-only summary functions and UI/export fields.
- Plugin framework: retain existing integration metadata while reverting the new
  registry helpers.
- ACI/CIMC: disable the scoped map while leaving unrelated inventory adapters.
