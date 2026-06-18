# APIC CIMC Inventory Readiness Audit

## Goal

Give operators a one-shot, read-only check for why the `Forward ACI APIC CIMC
Inventory` map produces zero `dcim.inventoryitem` rows: report whether the
synced snapshot's APIC devices actually carry the `moquery -c eqptCh -a all`
custom command the map parses, on a completed (non-backfilled) device.

## Constraints

- Read-only: runs the same Forward NQE/API path as a sync; no writes.
- Uses the sync's own source, network, and resolved snapshot so it reflects what
  the sync would see.
- No customer data, credentials, or network IDs in repo/tests; tests mock the
  client.

## Touched Surfaces

- `forward_netbox/management/commands/forward_apic_cimc_readiness_audit.py` —
  new management command.
- `forward_netbox/tests/test_apic_cimc_readiness_audit_command.py` — tests.

## Approach

Resolve the sync -> source client -> snapshot, run an NQE probe over APIC
devices reporting per-device `has_controller_detail`, `has_eqptch`, and
`completed`. Summarize counts and `cimc_inventory_ready` (a completed APIC with
eqptCh). When not ready, emit a remediation pointing at re-enabling the eqptCh
custom command as a recurring collection. `--fail-on-missing` exits non-zero for
monitoring.

## Validation

- `invoke test --test-label forward_netbox.tests.test_apic_cimc_readiness_audit_command`
  (ready / eqptCh-only-on-backfilled / fail-on-missing exit).
- `invoke harness-check`, `invoke lint`.

## Rollback

Delete the command and its test. No data, schema, or runtime-path impact.

## Decision Log

- Built as an on-demand management command rather than a sync-health-page
  addition to avoid per-page live snapshot scans; operators run it when CIMC
  inventory is unexpectedly empty.
- Mirrors the live CustomerOrg investigation that found eqptCh present only on
  collection-canceled (backfilled) APIC snapshots — this surfaces that exact
  condition (completed_with_eqptch == 0 while with_eqptch_command > 0).
