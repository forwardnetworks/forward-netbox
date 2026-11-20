# Rules table selects and NetBox's dropdown enhancer

## Goal

Make the Forward tag dropdown in the site rules table work in a real NetBox page. Found
when taking a screenshot against a production-sized tag list: NetBox wraps each plain select in its
own dropdown widget at page load; the wrapper copies the options it sees then, so the
333 tags added afterwards never appeared, the native select it hid never received the
focus that loads them, and the builder's hidden tag select left an empty visible dropdown.

## Constraints

- Plugin only, no queries, no migration. Storage and text format unchanged.

## Touched Surfaces

- `forward_netbox/widgets.py` (the four selects opt out with NetBox's `no-ts` class; the
  kind selects get a minimum width), `tests/test_site_rules_source.py`,
  `scripts/forward_ui_harness.mjs` (fails if NetBox wraps any rule select).

## Approach

`no-ts` is the class NetBox's enhancer skips. Native selects keep working with the lazy
tag load and later options.

## Validation

- 40 tests in `test_site_rules_source`; `invoke ui-test` rc=0.
- Live: local stack at 3.0.18 code with a production-sized tag list (333) and location list (491)
  served through mocked endpoints; the tag select lists the tags and the kind selects fit.
- Not verified: against a live Forward-connected source (the endpoints were mocked); the
  harness check was not broken once to prove it fails.

## Rollback

Revert the class; the dropdown loses its tag list again.

## Decision Log

- Opted out of the enhancer rather than syncing it after each load: one class, no
  dependency on the enhancer's internal API.
