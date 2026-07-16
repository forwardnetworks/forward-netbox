# Endpoint-aware scope reconciliation

Status: completed.

## Goal

Keep SNMP endpoints imported as NetBox devices, including Avocent console
servers, out of the Scope Reconciliation orphan set whenever the source's
endpoint-import settings would include them in a sync.

## Constraints

- Preserve the existing device-tag scope and backfill classifications.
- Match the bundled device query's endpoint rules: SNMP output required,
  exclude tags always applied, include tags applied only when
  `scope_endpoints_by_include_tags` is enabled.
- Fail closed when the endpoint scope probe fails so preview, tagging, and prune
  cannot classify endpoint devices from an incomplete source scope.
- Do not weaken the existing empty-device-scope prune guard.
- No migration or bundled NQE source change.

## Touched Surfaces

- `forward_netbox/utilities/scope_reconciliation.py`
- `forward_netbox/management/commands/forward_device_scope_reconciliation_audit.py`
- `forward_netbox/templates/forward_netbox/forwardsync_scope_reconciliation.html`
- `forward_netbox/tests/test_device_scope_reconciliation_audit_command.py`
- `forward_netbox/tests/test_scope_module_ui.py`
- User operations and troubleshooting documentation

## Approach

1. When `sync_endpoints` is enabled, query `network.endpoints` with the same
   endpoint predicates as sync-time scope resolution.
2. Union eligible endpoint names into the protected Forward name set and the
   in-scope/missing-from-NetBox comparison, while keeping device backfill
   accounting separate.
3. Report the endpoint count explicitly in the UI and audit payload.
4. Retain the modeled-device scope as a separate internal set and use it for
   destructive empty-scope guards.
5. Cover default endpoint inclusion, opt-in include-tag scoping, probe failure,
   and prune refusal when only endpoint scope resolves.

## Validation

- `invoke test-isolated --test-label='forward_netbox.tests.test_device_scope_reconciliation_audit_command forward_netbox.tests.test_scope_module_ui'`
  passed: 36 tests, 2 skipped, and zero Django system-check issues.
- `invoke harness-check` passed.
- `invoke harness-test` passed: 134 tests.
- `invoke lint` passed all hooks.
- `invoke check` passed with zero system-check issues.
- `invoke docs` built the documentation successfully.
- `git diff --check` passed.
- The focused view test verifies the rendered endpoint count. The full shared
  `invoke test` and Playwright suite were not run because the shared development
  database contains unrelated active jobs; isolated coverage exercises the
  changed behavior without mutating that state.

## Rollback

Revert the helper, endpoint union, UI row, tests, and docs. No schema or stored
state cleanup is required; maintained scope tags converge on the next job run.

## Decision Log

- Reusing only `network.devices` was rejected because Forward SNMP endpoints are
  intentionally not first-class modeled devices.
- Treating endpoint probe failure as an empty endpoint set was rejected because
  it would recreate the false-orphan condition and could make prune destructive.
- Endpoint names do not count as evidence for the empty modeled-device prune
  guard; this prevents a partial device-scope failure from being masked by a
  successful endpoint query.
