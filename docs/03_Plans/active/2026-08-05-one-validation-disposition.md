# One validation disposition for both apply paths

## Goal

The interface tranche taught the bulk engine to skip a rejection about state the
row does not write. The row-oriented adapter kept failing the same row, so the
two apply paths reached opposite conclusions about the same interface while each
was locally correct. Make it one predicate, used by both.

## Contract

- `is_preexisting_rule_rejection(exc, written_fields)` is the only place the
  question is answered. Both apply paths call it; neither reimplements it.
- A skip does not mark the row's dependents failed. The object still exists —
  the sync only declined to change it — so its IP and MAC rows still resolve.
- A path that does not say what it wrote keeps failing. Inferring "not ours"
  from silence is the wrong default.

## Constraints

- The adapter validates inside `coalesce_update_or_create`, which every model
  goes through. The disposition therefore cannot be decided there without
  deciding it for everything, so the writer attaches what it wrote and the
  generic recorder decides.
- Only a catalogued rule can produce a skip, and the catalogue is seven rules.
  That is what keeps a change to the generic row loop from being a change to
  every model's failure handling.
- `full_clean` on create validates an object whose every field the row wrote, so
  a create can never be pre-existing state. Said explicitly rather than inferred.
- No customer identifiers.

## Touched Surfaces

- `forward_netbox/utilities/diagnostics.py` — the predicate
- `forward_netbox/utilities/sync_primitives.py` — attach `forward_written_fields`
  on both the create and update `full_clean`
- `forward_netbox/utilities/sync_reporting.py` — the generic row-loop catch
- `forward_netbox/utilities/apply_engine_bulk.py` — drop the inline copy
- `forward_netbox/tests/test_sync.py`,
  `forward_netbox/tests/test_merge_rule_rejection.py`

## Approach

The exception carries the answer to the one question the recorder cannot derive:
which fields this change wrote. `full_clean` validates the whole object, so the
rejection alone cannot distinguish "we wrote something invalid" from "something
invalid was already there". Everything else — is the rule catalogued, does the
rejected field intersect what was written — is derivable and lives in the
predicate.

## Validation

The adapter parity test drives the same cross-site untagged VLAN through
`_apply_model_rows` and asserts both the `skipped` outcome and that the
dependency is NOT marked failed. Five predicate unit tests cover the written /
unwritten split, an uncatalogued rule, a non-`ValidationError`, and an empty
written set. Full plugin suite run, not just the touched files, because the
change lands in the generic row loop.

## Rollback

Revert. No migration, no persisted shape change; a reverted build returns every
rejection to `failed`.

## Decision Log

- **The writer attaches the field set; the recorder decides.** The alternative
  was deciding disposition inside `coalesce_update_or_create`, which would have
  put model-specific policy in the one function every model shares.
- **`getattr(exc, "forward_written_fields", None)` defaults to failing.** Any
  raiser that has not been taught to say what it wrote keeps today's behaviour,
  so this cannot silently downgrade a path nobody reviewed.
- **The bulk path lost its inline copy rather than keeping a fast local one.**
  Two implementations of this question is exactly what produced the divergence.

## Closed

- **Task #39 is done** (`bb0ac0d`, #146). `_is_destination_rule_rejection`
  (`merge.py:68`) now reads `exc.forward_written_fields` and defers to
  `is_caused_rule_rejection` (`diagnostics.py:901`). The written set is
  attached at `bulk_merge.py:627` and, on the row-oriented adapter path, at
  `sync_primitives.py:145` (create) and `:180` (update); the adapter's own
  disposition reads it at `sync_reporting.py:723`, so both engines reach the
  same verdict on the same row.

  This was carried as open in three later release plans after it had already
  shipped. Verified in the 2.9.2 tree before closing.

## Open

- The catalogue is seven rules, so most rejections still cannot be skipped even
  when they are genuinely pre-existing. Growing it is a per-rule decision, made
  when a rule actually shows up in the field, not speculatively.
- Batched status-only and relationship-fallback sites swallow into a row-by-row
  retry, so their disposition is decided on the retry rather than at the batch.
