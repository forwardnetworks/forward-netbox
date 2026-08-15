# Name the row a merge rejection was about

## Goal

An operator reading a recorded merge rejection can open the NetBox object it is
about. Today the record names the model and the rule and stops there, so the
remedy - editing the offending row - starts with a hunt.

The field report in #206 reads:

> Merge for ipam.ipaddress failed (ValidationError) violating
> primary-ip-reassignment-blocked. Recorded and skipped: re-running cannot
> change a NetBox validation rejection, so the baseline was promoted over this
> row.

Accurate, unactionable. One `ipam.ipaddress` row out of a 4,000-device sync is
being talked about and nothing says which.

## Constraints

- **No customer value may be persisted.** This module refuses that deliberately
  and in three places: `diagnostic_shape` keeps field names and discards their
  values, `redacted_message_shape` exists for the same reason, and the merge
  recorder's own comment says the key values a Postgres `DETAIL` line embeds
  "are deliberately still not captured". An IP address, a device name and an
  interface name are customer values. Whatever names the row must not be one.
- **No message may change for a caller that has no row in hand.** The
  orchestration paths record whole-model failures, and `MergeIssueRecorderTest`
  pins one such message by exact equality.
- **`raw_data` keys must stay disjoint from the diagnosis** so anything already
  reading them is unaffected.
- No migration: `ForwardIngestionIssue.raw_data` is already a `JSONField`.

## Touched Surfaces

- `forward_netbox/utilities/merge.py` - `_row_identity` (new),
  `_MergeIssueRecorder.record`, `_record_failed`
- `forward_netbox/tests/test_merge_rule_rejection.py` - `MergeRowIdentityTests`
  (new), one end-to-end assertion on the field-reported rejection
- `forward_netbox/tests/test_ingestion_merge.py` - `MergeIssueRecorderTest`

## Approach

The identity was never missing. `_record_failed` already holds the
`CollapsedChange`, whose `key` is `(label_lower, pk)` - the same `key[1]` the
authoritative-delete guard logs in the clear forty lines below. It simply was
not passed to `record()`, which took `model_string` and `exc` and nothing else.

`record()` gains two optional keyword arguments, `pk` and `change_data`, both
defaulting to `None`. `_row_identity(pk, change_data)` returns `None` when there
is no pk, and the whole feature is then a no-op - that is what keeps the
whole-model messages byte-identical.

**What is reported is the NetBox primary key, not the row's values.** The pk is
a NetBox-assigned surrogate; it resolves to the object page in the operator's
own NetBox, which is exactly where the edit has to happen. It answers "which
row" completely while disclosing nothing, so the redaction boundary above stays
intact. The message gains one sentence:

    ... over this row. Affected NetBox row: pk 900002.

`raw_data` gains `row_pk` (the pk, as a string - some models key on a UUID) and
`row` (the change data through `diagnostic_shape`, so field names only). That is
the same split the sync-phase recorder already uses, where `raw_data` is
`{**diagnostic_shape(row), **diagnosis}`.

A branch-native DELETE carries no `postchange_data`, so it records the pk and no
shape.

## Validation

- `MergeRowIdentityTests` (`SimpleTestCase`, no database): no pk leaves the
  message unchanged; an int pk renders and the change data reduces to field
  names; **no customer value reaches the message or the shape**, asserted
  directly; a UUID pk survives and a delete records no shape.
- `test_the_skipped_row_names_the_netbox_object_to_edit` drives the real
  `sync_merge` path with the field-reported `primary-ip-reassignment-blocked`
  and asserts the pk reaches both `message` and `raw_data["row_pk"]`, that the
  rule is still named, and that the ValidationError's own text still is not.
- `test_a_recorded_row_names_its_pk_and_records_its_shape` pins the exact
  message and the exact `raw_data`.
- `MergeIssueRecorderTest.test_module_bay_failures_...` is unchanged and still
  asserts `"Merge for dcim.modulebay failed (Exception)."` by equality, which is
  the regression test for the no-pk path.
- `invoke lint` (black 25.9.0, flake8 7.3.0), `invoke harness-check`,
  `invoke test`.

## Rollback

Revert. `record()`'s new arguments are optional and nothing else calls it, so
the previous messages and `raw_data` return exactly.

## Decision Log

- **The pk, not the address.** #206 asks for the address, device and interface.
  Naming the value would put customer data into a record that reaches support
  bundles, against an invariant this module enforces everywhere else. The pk
  gets the operator to the same page. If Forward decides the value is wanted
  too, `_row_identity` is the one function to change and the tests that assert
  the absence are the ones to update - deliberately, not by accident.
- **A generic identity, not an `ipam.ipaddress` special case.** The report came
  from one model; the gap is in the recorder, and every merge rejection has the
  same question behind it.
- **Appended, not interpolated into the prefix.** `safe_operation_failure`
  writes the machine-readable head that `recovered_classifiers` reads back out.
  Adding to it would change every merge message and break readback; a trailing
  sentence is additive.

## Open

- **The Drift Summary half of #206 is NOT addressed here.** `EXACT_COMPARISON`
  is defined and never produced, so `In sync` / `Drifted models` / `Total drift`
  read "Not measured" on every run - confirmed: the only producer,
  `_dependency_dry_run_payload`, hardcodes `workload_upper_bound`
  (`views.py:507-508,563`) and `drift_report.py:136,176` gates on the constant.
  #206's own fix direction offers two answers - compute a real comparison, or
  relabel the panel as the workload estimate it is - and choosing between them
  is Forward's call, not a drive-by. #206 should stay open for it.
