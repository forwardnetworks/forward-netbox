# Refuse an orphan prune when the scope collapsed

## Goal

Stop a partially narrowed Forward scope result from deleting valid NetBox
devices, and stop the audit telling operators to delete before they have
checked why the devices left scope.

## Contract

- The guard refuses BEFORE any delete. A prune is not partially recoverable.
- It must not fire on ordinary attrition or on small deployments, because a
  guard that fires on normal operation is a guard that gets switched off.
- The override exists and is reachable from the CLI. An override that cannot
  fire is worse than none.

## Constraints

- `out_of_scope = (previously_managed_names & netbox_names) - tagged_names`.
  Membership is decided purely by ABSENCE from the current result, so any
  under-returning query manufactures orphans out of valid devices.
- The only prior guard refused at zero devices returned. A result holding most
  of the fleet passed it cleanly.
- Refusal text is persisted to a job record, so it may carry counts but never a
  device name.

## Touched Surfaces

- `forward_netbox/utilities/scope_reconciliation.py`
- `forward_netbox/jobs.py`
- `forward_netbox/management/commands/forward_device_scope_reconciliation_audit.py`
- `forward_netbox/tests/test_scope_shrink_guard.py`

## Approach

Expose `forward_previously_managed` as the denominator, then refuse when the
orphan set is both past `SAMPLE_LIMIT` in absolute terms and more than a
quarter of what the sync previously claimed. Both conditions are required.

## Validation

- `invoke test-isolated --test-label=forward_netbox.tests.test_scope_shrink_guard` - 9 tests, OK
- `...test_device_scope_reconciliation_audit_command` - 20 tests, OK
- `...test_button_jobs` - 13 tests, OK

## Rollback

Revert. The prune returns to refusing only on a zero-device result.

## Decision Log

- **A ratio alone was wrong and a real fixture caught it.** The first version
  refused on ratio only, which broke
  `test_prune_orphans_apply_deletes_only_out_of_scope`: three orphans out of
  eight claimed is 38%. That is not a near-miss in a test, it is what a lab or
  a small sync looks like, and the guard would have fired on them in
  production. The absolute floor is the fix, and `SAMPLE_LIMIT` is the
  principled line - at or below it the report already names every orphan, so
  the operator can review the whole blast radius by eye.
- **This deliberately does NOT block the case that prompted it.** The customer
  had 38 orphans against ~3400 previously claimed, 1.1%. Blocking that would
  train operators to pass the override reflexively. What protects that case is
  the corrected remediation text, which now says to confirm in Forward before
  deleting.

## Open

- Whether those 38 genuinely left the include tags is still unconfirmed, and is
  a question for Forward's tag membership rather than for this code.
