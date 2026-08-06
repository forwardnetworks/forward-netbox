# Catalogue the tag-side ownership conflicts

## Goal

Make a refused scope-tag reconciliation say which rule refused it, instead of
`unrecognized-ownership-conflict`.

## Contract

- The refusal messages embed tag names and source device keys. For this
  customer the tags are people's names, so the message must never be
  persisted - only a slug this module defines.
- An unmatched message still records that it was unrecognised. Falling silent
  is worse than an honest unknown.

## Constraints

- `ownership_conflict_reason` is the single formatter every failure path runs
  through. Enriching a call site instead is what made the first attempt at this
  pass its own tests while changing nothing the customer saw.

## Touched Surfaces

- `forward_netbox/utilities/diagnostics.py`
- `forward_netbox/tests/test_ownership_conflict_reason.py`

## Approach

`OwnershipConflictError` is raised from nine sites. Four were catalogued and
all four are device-identity refusals raised while merging an ingestion. The
five raised while materializing tags had no slug, so the entire scope-tag
reconciliation job could only ever report `unrecognized-ownership-conflict`.

Add a slug per tag-side site, then read the raise sites out of the modules with
`ast` and assert each one resolves. Listing conditions by hand is precisely
what let five of nine go missing.

## Validation

`invoke test-isolated --test-label=forward_netbox.tests.test_ownership_conflict_reason`
- 7 tests, OK.

## Rollback

Revert the commit. The slugs are additive; nothing reads them but the job
record and the UI error string.

## Decision Log

- **Matched on `is already controlled as` rather than `cannot also be`.** The
  latter is short enough to collide with an unrelated future message.
- **The structural test walks the AST rather than importing and raising.** The
  raise sites need database state and a live tag to reach, so constructing them
  for real would test the setup, not the catalogue.

## Open

- The customer's specific conflict is still unidentified: the console buffered
  the diagnostic out twice and no further probing was warranted. This change
  means the next occurrence names itself, which is the durable answer.
