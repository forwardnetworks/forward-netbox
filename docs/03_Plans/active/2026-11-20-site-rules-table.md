# Site rules as a table

## Goal

Replace the one-rule-per-line box as the way an operator edits site mapping rules with a
table: a row per rule, a kind select, a Forward tag select (from Forward) or a location
field (with Forward's location names as suggestions), and a NetBox site field (with the
existing NetBox sites as suggestions). Adding and removing a row are buttons.

## Constraints

- Storage, validation and the text format do not change. The textarea stays the posted
  field, rewritten from the rows on every change, so a browser without the script still
  saves rules by typing lines.
- A line the table cannot show leaves the text editor in charge; nothing is dropped.
- A rule may name a NetBox site that does not exist yet (the sync creates it), so the site
  field stays free text; the NetBox sites are suggestions only (capped at 2000).
- Plugin only: the queries do not change, so no republish.

## Touched Surfaces

- `forward_netbox/widgets.py` (table markup, script, site suggestions).
- `forward_netbox/tests/test_site_rules_source.py`, `scripts/forward_ui_harness.mjs`.

## Approach

The script parses the textarea into rows on load and after the builder adds a line. Row
edits call `writeBack`, which writes only complete rows (value and site) back as lines.
Tags load once, lazily, shared by every tag row and the builder.

## Validation

- `invoke test-isolated --test-label forward_netbox.tests.test_site_rules_source`
- `invoke ui-test` (rows render from text, edit rewrites the field, remove clears it).
- Full gate at the 3.0.18 cut.

## Rollback

Revert the widget; the textarea alone is the 3.0.17 behaviour.

## Decision Log

- Kept the builder above the table rather than removing it: it is the first-use path and
  the existing browser checks cover it; the table is the editor for what is saved.
