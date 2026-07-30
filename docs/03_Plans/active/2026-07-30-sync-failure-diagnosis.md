# Sync Failure Diagnosis

## Goal

Make a failed sync explicable from the UI alone. No server logs, no CLI.

A customer's 2.6.6 sync stopped on a `dcim.module` `IntegrityError`. Every
surface — the issue list, the API, a support bundle — said "row processing
failed (IntegrityError)" and nothing more. The only way to learn which
constraint was violated was to read the worker log on their host, which is
precisely what the issue records exist to avoid.

## Contract

- A sync-phase failure records the schema-level diagnosis: constraint, table and
  column names for an `IntegrityError`; invalid field names for a
  `ValidationError`.
- The **terminating** failure records it too — it previously stored `{}`.
- The diagnosis appears in the issue message, so the list view is enough for the
  common case, and in `raw_data`, so the API and support bundle carry it.
- Submitted values are never recorded. Only schema identifiers.
- An exception with nothing to add leaves the message exactly as it was.

## Constraints

- Persisted diagnostics stay free of customer data — this is the constraint the
  original empty `raw_data` was protecting, and it must not be traded away for
  legibility.
- Do not change what the exception classes mean or how failures are counted.

## Touched Surfaces

- `forward_netbox/utilities/sync_reporting.py` — per-row issues
- `forward_netbox/utilities/sync_orchestration.py` — the terminating failure
- `forward_netbox/tests/test_issue_diagnosis.py`
- `forward_netbox/tests/{test_sync,test_models}.py` — pinned expectations
- This plan.

## Approach

`structured_failure_diagnosis` already existed, tested, and captured exactly the
right things — psycopg's `diag` constraint/table/column, and a
`ValidationError`'s field names, while rejecting anything that is not a plain
schema token. 2.6.6 wired it into `merge.py` **only**.

Sync-phase failures are the common case and had none of it. Two call sites:

1. `sync_reporting.record_issue` — the per-row recorder. Message becomes
   `<model> row processing failed (IntegrityError; constraint <name>).` and
   `raw_data` merges the diagnosis alongside the existing row shape. The keys
   are disjoint (`type`/`fields` vs `exception_type`/`constraint_name`/…), so
   anything already reading the shape is unaffected.
2. `sync_orchestration._record_forward_sync_failure` — the terminating failure,
   which hardcoded `raw_data={}`. This is the row an operator lands on when a
   run dies, and it named no model and carried no detail, so the single most
   important issue in the UI was the least informative.

## Validation

- `test_issue_diagnosis`, `test_issue_rendering`, `test_sync`, `test_models`,
  `test_ingestion_merge`, `test_logging`: **471 tests, OK.**
- **Negative control:** with `diagnosis` forced to `{}`, three of the new tests
  fail. Restored and re-run green.
- Redaction is asserted in both directions: a `ValidationError` whose message
  quotes a device name must not put it in the message or `raw_data`, and a
  `RuntimeError` must leave the message byte-identical to before.

## Rollback

Revert the commit. Additive: two call sites gain a diagnosis, and messages gain
a clause only when there is something to say.

## Decision Log

- 2026-07-30: Put the constraint in the **message**, not only in `raw_data`. The
  issue list shows the message; requiring an operator to open each row to find
  the constraint is a smaller version of the same problem.
- 2026-07-30: Kept the redaction test's exact-equality assertion and updated the
  expected dict, rather than relaxing it to a subset check. Exact equality is
  what catches an unintended field arriving later; loosening it to accommodate
  an intended addition would disarm it permanently.
- 2026-07-30: Message text only extends when a constraint or invalid field is
  present. An exception with nothing to add must not gain trailing punctuation
  suggesting detail that is absent.

## Evidence

- Reported failure surface, 2026-07-30: three sync-phase issues — a
  `dcim.module` `IntegrityError` with coalesce context `{"fields": []}`, a
  downstream `netbox_dlm.softwareversion` `ForwardDependencySkipError`, and a
  terminating `IntegrityError` with a blank model and empty context.
- `dcim_module` carries exactly two unique constraints — `dcim_module_asset_tag_key`
  and `dcim_module_module_bay_id_key` (one module per bay) — plus four foreign
  keys. Which of those was violated is the question the old record could not
  answer and the new one does.
- The accept-merge-failures escape hatch does **not** apply to these:
  `can_accept_merge_failures` requires `can_queue_merge`, which requires a
  branch at `READY_TO_MERGE`. A sync that dies before merging never gets there.
