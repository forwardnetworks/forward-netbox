# Scope tag slug collision blocks ownership reconciliation

## Goal

Stop a benign tag-slug collision from leaving the whole ownership domain
Incomplete, and stop the lookup missing a tag that already exists under the
other slug convention.

## Contract

- Creation still derives exactly ONE slug per name. This changes resolution,
  not what gets written.
- Tag names here are people's names, so a refusal reports tag ids, never names.
- A row this plugin already manages is never silently abandoned: switching away
  from it would strand its claims, so that case stays a refusal.

## Constraints

- `slugify` DROPS a dot rather than replacing it, so `slugify("A.Person")` is
  `aperson` and is truthy. The `or slugify(name.replace(".", "-"))` arm in
  `normalized_managed_tag_slug` is therefore unreachable for any name that
  survives slugify at all - it fires only for names with no ASCII word
  characters. Five call sites duplicate that same dead expression.

## Touched Surfaces

- `forward_netbox/utilities/tag_contracts.py`
- `forward_netbox/utilities/ownership.py`
- `forward_netbox/tests/test_scope_tag_slug_collision.py`

## Approach

Add `candidate_managed_tag_slugs`, the set of slugs a tag for a given name may
already be stored under, and use it for RESOLUTION only.

`_locked_scope_tag` then resolves in order: no name match, use a slug match;
name match whose own slug is a candidate, use it (nothing to be ambiguous
about); otherwise a colliding row decides - refuse only if it is
plugin-managed, else the configured name wins.

The operator configures a scope tag by name. The slug is derived from that
name, so when the two disagree the name is the stronger evidence of intent.

## Validation

- `invoke test-isolated --test-label=forward_netbox.tests.test_scope_tag_slug_collision` - 9 tests, OK
- `...test_device_scope_tagging` - 11 tests, OK
- `...test_ownership_migration` - 3 tests, OK
- `...test_device_scope_reconciliation_audit_command` - 20 tests, OK

## Rollback

Revert. Resolution returns to refusing whenever a name match and a slug match
differ.

## Decision Log

- **The first test asserted a refusal that correctly did not happen.** With the
  named tag holding `a-person`, that tag satisfies both lookups by itself and
  there is no ambiguity. A genuine collision needs the named tag to carry
  NEITHER candidate slug, which NetBox permits since slugs are free-form. The
  code was right and the test premise was wrong; both cases are now pinned.
- **Refuse only on a managed collision.** An unrelated tag that merely happens
  to hold the derived slug has no claim on the name and must not veto it.

## Open

- The customer's specific conflict was never confirmed - the nbshell probe
  buffered out twice and further probing was not warranted. This removes the
  most likely cause and [[the catalogue change]] makes any recurrence name
  itself.
- The dead `.`-to-`-` expression still appears at four other call sites
  (`sync_device.py:176`, `apply_engine_bulk.py:2067`, migration 0035, and
  `tag_contracts` itself). They all compute the same value as before, so this
  is tidying rather than a fix.
