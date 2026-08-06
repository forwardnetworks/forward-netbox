# Narrow the merge classifier without wedging a baseline

## Goal

`_is_destination_rule_rejection` is `isinstance(exc, ValidationError)`. Every
validation rejection at merge is therefore unsatisfiable and skipped, including
rejections the merge itself caused, where a retry is exactly the right response.
Name those, and only those.

## Contract

- A rejection is a real failure only when a rule the catalogue can name lands on
  a field this change writes.
- An uncatalogued rule stays unsatisfiable. So does a caller that never said
  what it writes.
- The rejection this classifier was built for — `ipam.ipaddress` on `__all__` —
  keeps its disposition, because `__all__` cannot intersect a written field.

## Constraints

- Any failed row blocks baseline promotion outright, with no self-service escape
  short of `forward_accept_merge_failures`. Widening what counts as a failure is
  therefore the dangerous direction, and it is the direction this change moves.
  That is why only catalogued rules can move, and the catalogue holds seven.
- A customer is mid-incident on exactly this surface. The change must not be
  able to wedge a baseline on a rule nobody has catalogued.
- No customer identifiers.

## Touched Surfaces

- `forward_netbox/utilities/diagnostics.py` — `is_caused_rule_rejection`
- `forward_netbox/utilities/bulk_merge.py` — `_full_clean_fast` gains
  `written_fields`; create, update, and batched-relationship sites pass it
- `forward_netbox/utilities/merge.py` — the classifier
- `forward_netbox/tests/test_merge_rule_rejection.py`

## Approach

Same mechanism as the sync path: the writer attaches what it wrote, because the
recorder cannot derive it from an exception raised by whole-object validation.
The merge already computes the set — `scalar_changes` on update,
`postchange_data` on create, `changed` in the batched relationship path.

**The predicate is deliberately not the sync path's.** `is_caused_rule_rejection`
is not the complement of `is_preexisting_rule_rejection`; both are False for an
uncatalogued rule. The two paths have opposite defaults — sync fails unless it
can name a reason to skip, merge skips unless it can name a reason to fail — so
each has to answer for itself. Inverting one to get the other would flip every
uncatalogued merge rejection to a failure.

## Validation

Five classifier tests pin the asymmetry: a catalogued rule on a written field is
retryable; the same rule on an unwritten field is not; an uncatalogued rule stays
unsatisfiable *even on a written field*; a caller that attaches nothing stays
unsatisfiable; and the customer's `__all__` network-ID rejection still skips.
Full plugin suite, since the change reaches the merge apply path.

## Rollback

Revert. No migration. A reverted build returns every validation rejection at
merge to unsatisfiable, which is strictly the safer direction, so this can be
backed out under incident conditions without further thought.

## Decision Log

- **Inverted rather than symmetric.** The literal reading of the task was to use
  the sync predicate here. Rejected on consequence: uncatalogued rejections
  would flip from skipped to failed, and a failed row blocks promotion with no
  escape — reintroducing the dead-end that the unsatisfiable disposition exists
  to prevent, for a customer currently living on this code path.
- **`written_fields=None` means unsatisfiable.** Every merge path not yet taught
  to attach the set keeps today's behaviour, so the blast radius is the three
  sites that were changed and nothing else.

## Open

- The catalogue is seven rules, so most merge rejections the plugin genuinely
  caused still present as unsatisfiable. Growing it is a per-rule decision made
  when a rule shows up in the field.
- The batched paths at the status-only and relationship-fallback sites swallow
  their exceptions into a row-by-row retry, so their disposition is decided on
  the retry, not at the batch. Left alone; noting it so the next reader does not
  assume every `full_clean` in this file feeds the classifier.
