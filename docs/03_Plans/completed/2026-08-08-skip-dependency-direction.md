# Say which way a skipped row's dependency points

## Goal

Stop two opposite conditions reading identically in the ingestion issue list.

## Contract

- Only schema identifiers are persisted. The direction is a property of the
  raiser, not of any customer value.
- A raiser that names no dependency records exactly what it did before.

## Constraints

- The delete path is the inverse of every other skip, and the raiser already
  says so in a comment - the wording just never reached the persisted row.
- `diagnostic_shape` keeps dict KEYS and drops VALUES, which is why the
  dependency travels as an exception attribute rather than in `context`.

## Touched Surfaces

- `forward_netbox/exceptions.py`, `forward_netbox/utilities/sync_primitives.py`,
  `forward_netbox/utilities/sync_reporting.py`
- `forward_netbox/tests/test_skip_dependency_direction.py`

## Approach

Carry `dependency_is_protecting` on the exception, set it at the one delete-path
raiser, and phrase the detail as "waiting on X" or "still referenced by X".
Extract `dependency_phrase` so the choice is unit-testable rather than buried in
a long `elif` chain.

## Validation

- `invoke test-isolated` - full plugin suite, 2002 tests, OK (4 skipped)
- `invoke test-isolated --test-label=...test_skip_dependency_direction` - 4 OK

## Rollback

Revert. Both directions return to naming the model with no relationship.

## Decision Log

- **Found from a customer's real rows, not from reading code.** Their ingestion
  recorded `netbox_dlm.softwareversion row processing skipped (...;
  netbox_dlm.inventoryitemsoftware)`. As a missing parent that is backwards -
  `inventoryitemsoftware` depends on `softwareversion` - which is what made the
  inversion visible. The rows were a prune correctly refused, but nothing in
  the message said so.
- **Extracted a helper rather than testing through `record_issue`.** The
  composition sits in a long `elif` chain needing database state to reach; the
  phrasing choice is the whole behaviour and deserves a direct test.

## Open

- Roughly two dozen raisers still name no dependency at all and record only the
  exception class. The vocabulary exists in `record_aggregated_skip_warning`
  and never reaches the database. Unchanged here.
