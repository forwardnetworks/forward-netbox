# Stop one interface from failing an entire sync, and say what rejected it

## Goal

Customer ingestion 2719 on 2.7.2 staged 172 changes, failed the whole sync, and
produced one issue row: phase `sync`, model blank, `Forward ingestion failed
(ValidationError) on invalid field(s) untagged_vlan.` Three defects behind that
one row, fixed together because each one hides the next.

## Contract

- A row NetBox refuses is attributed to `dcim.interface`, names the device and
  the interface, and does not stop the shard.
- A rejection is recorded with the RULE that fired, not only the field it
  landed on. `untagged_vlan` carries two unrelated NetBox rules.
- A rejection on state the row does not write is skipped, not failed: it is not
  retryable and it is not ours, and a failure fails the row's dependents.
- No message text is persisted. Rule slugs and field names only, with
  `redacted_message_shape` for anything uncatalogued.

## Constraints

- `full_clean` validates the whole object and `exclude` does NOT filter errors
  raised by `Model.clean()` — Django merges them into the error dict verbatim.
  So field-scoping the validation call cannot avoid this; the disposition has
  to be decided after the rejection, not before it.
- `bulk_update` bypasses `save()` and `clean()`. That is how the invalid state
  gets there in the first place (moving a device's site never revalidates its
  interfaces) and it is why validating here is voluntary rather than structural.
- The tail loop counts one statistic per appended outcome slot, so a call site
  that already appended one must not also increment directly.
- No customer identifiers.

## Touched Surfaces

- `forward_netbox/utilities/diagnostics.py` — rule catalogue and reader
- `forward_netbox/utilities/sync_reporting.py` — issue detail composition
- `forward_netbox/utilities/apply_engine_bulk.py` — `_validate_interface` and
  its four call sites
- `forward_netbox/tests/test_merge_rule_rejection.py`,
  `forward_netbox/tests/test_sync.py`

## Approach

**Rule catalogue reads every field, not just `__all__`.** `__all__` is the
absence of a field name, which is why it was the only case the catalogue
covered; a field name is equally silent about which rule fired. The catalogue
gains the two NetBox interface rules, and `describe_failure` and the issue
recorder report the field and the rule together instead of choosing.

**`_validate_interface` captures instead of propagating.** It returns `"ok"`,
`"skipped"` or `"failed"`, records the issue itself so four call sites cannot
describe it differently, and leaves counting to the caller, which is the only
one that knows whether an outcome slot exists.

**Disposition is decided by what the row writes.** Each call site passes the
fields it is writing. A catalogued rule on a field outside that set is
pre-existing state: recorded, skipped, dependents untouched. Anything else —
including an uncatalogued rule — stays a failure.

## Validation

Two new sync tests build the real shape via `queryset.update()`, since `save()`
and `full_clean()` both refuse to create it: a cross-site untagged VLAN on an
existing interface is skipped while an unrelated interface in the same shard is
still created, and an out-of-range MTU on a field the row does write still
fails. Plus five diagnostics tests, including that the two `untagged_vlan`
rules are distinguishable and that values in a field-scoped message are still
masked.

## Rollback

Revert. The change is confined to failure handling; nothing migrates and no
persisted shape changes except issue `message`/`raw_data` content.

## Decision Log

- **Skip rather than write around the violation.** We could write only the
  changed fields with `bulk_update`, which bypasses validation anyway, and
  leave the pre-existing violation in place. Refused: the object would be
  written while known-invalid, and the operator would get no signal. Skipping
  costs that interface's MTU update until the VLAN is fixed, and says so.
- **Uncatalogued rules stay failures.** Skipping is the disposition for a
  rejection we understand and did not cause. Treating anything we cannot name
  as someone else's problem is how a real defect gets quietly downgraded.
- **The member row is what a LAG parent rejection is recorded against.** A
  synthesised parent has no row of its own; that is the only attribution there
  is, and it is better than none.

## Open

- **The adapter path was left alone and now disagrees.** A test written against
  `_apply_model_rows` turned out to exercise the row-oriented adapter, not the
  bulk engine — it logged `Failed applying dcim.interface row
  (ValidationError).` and had to be retargeted. The adapter validates inside the
  generic `_upsert_values_from_defaults`, so applying this disposition there
  changes failure handling for every model, not just interfaces; that needs its
  own blast-radius review. `test_bulk_adapter_parity` compares final DB state
  and both paths write nothing in the skip case, so it does not catch the
  divergence — a test covering the interface's dependents would. Tracked as #50.
  The customer's failure was the bulk path (an issue with no model at all is
  only produced by the uncaptured bulk path), so the fix is not blocked on it.
- `_is_destination_rule_rejection` in the merge path is still
  `isinstance(exc, ValidationError)` — every validation rejection at merge is
  treated as unsatisfiable. The predicate added here (a catalogued rule on a
  field the change does not write) is the narrowing that task #39 asks for, and
  should replace it. Left out of this change because the merge path has no
  notion of "fields this row writes" and needs its own derivation from the
  change diff.
- Which of the two `untagged_vlan` rules the customer actually hit is still
  unknown. This change is what makes the next occurrence say so.
- The plugin moves a device between sites without revalidating that device's
  interfaces, which is the mechanism that produces the cross-site pairing.
  Detecting it at the point it is created, rather than at the next sync that
  touches the interface, is not addressed here.
