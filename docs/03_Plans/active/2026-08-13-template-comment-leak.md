# Stop a multi-line template comment rendering as visible text

## Goal

Remove explanatory notes that Django emitted verbatim into the Scope
Reconciliation panel and the ingestion merge button, and make the mistake
impossible to repeat.

## Constraints

- The fix must not change any rendered output other than removing the leaked
  text.
- The guard has to run somewhere that fails a change before it ships. The full
  suite passed with the comment on screen, so a test asserting the page renders
  is not sufficient.

## Touched Surfaces

- `forward_netbox/templates/forward_netbox/forwardsync_scope_reconciliation.html`
- `forward_netbox/templates/forward_netbox/partials/ingestion_merge_button.html`
- `scripts/check_harness.py` - `_check_template_comments_are_parseable`
- `scripts/tests/test_check_harness.py`

## Approach

Django's `{# #}` comment syntax is single-line only. A comment broken across two
lines is not parsed as a comment; the engine emits it as literal text. Both
offenders were written as wrapped prose, which is exactly the shape that trips
it - a short comment stays on one line and works, and a considered one wraps and
leaks.

A customer screenshot of the panel showed the note about absence classification
rendered as body copy beside the badge it was explaining. The other instance, in
the merge button partial, predates this work and had been leaking unnoticed.

Both become `{% comment %}...{% endcomment %}`, which is multi-line by design.

The guard is a harness check rather than a rendering test. Nothing about the
page was broken: the template rendered, the view returned 200, and 2085 tests
passed with the text on screen. Only reading the output catches it, and no test
reads the output looking for template syntax. A lint over the template source is
cheap, exact, and fails before review.

## Validation

`scripts/check_harness.py` reports both offenders before the fix and none after;
tests in `scripts/tests/test_check_harness.py` pin that a wrapped comment is
reported and a single-line one is not.

The scan looks at the LAST `{#` on a line, not the first. Splitting on the first
lets a closed comment vouch for an unclosed one beside it - `{# a #} {# b` would
pass - which is a hole in a guard whose whole value is having none.

## Rollback

Revert. The notes leak again and the guard stops running.

## Decision Log

- **Harness check, not a template test.** The defect is invisible to every
  signal the suite already produces; the only reliable detector is the source
  itself.
- **Keep the comments rather than delete them.** They explain why the badges
  exist, which is worth more than the two lines cost.

## Open

- Nothing. Both instances are fixed and the class is now guarded.
