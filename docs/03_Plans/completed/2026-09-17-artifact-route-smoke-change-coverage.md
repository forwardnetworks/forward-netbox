# Cover ForwardChange/ForwardChangePolicy in the artifact route smoke probe

## Goal

`scripts/validate_installed_routes.py` (the `artifact-upgrade-test` pre-task
of `invoke ci`) fails on every run since `ForwardChange` and
`ForwardChangePolicy` were added: `_require_menu_coverage()` refuses to start
because the plugin's own menu (`forward_netbox/navigation.py`) links
`forwardchange_list` and `forwardchangepolicy_list`, and `MENU_ROUTES` was
never updated to match. Restore a clean `invoke ci` run by adding fixture
coverage for both models, plus their child `ForwardChangePolicyRule`, whose
`_changelog`/`_delete`/`_edit` detail routes are reachable by URL name even
though it has no menu entry of its own.

## Constraints

- `MENU_ROUTES` stays hand-written (per the file's own docstring): each
  entry also asserts a fixture row actually rendered, which cannot be
  derived from the menu declaration alone.
- The `fixtures` dict used for detail-route enumeration matches by longest
  prefix (`fixture_model_for`), so `forwardchange`, `forwardchangepolicy`,
  and `forwardchangepolicyrule` must all be present as distinct keys or the
  policy-rule's routes silently borrow the wrong model's pk and 404.

## Touched Surfaces

`scripts/validate_installed_routes.py` only.

## Approach

- Import `ForwardChange`, `ForwardChangePolicy`, `ForwardChangePolicyRule`.
- Add two `MENU_ROUTES` entries (`forwardchange_list`, `forwardchangepolicy_list`)
  with distinct fixture text.
- Create one fixture row per model in `main()`: a `ForwardChangePolicy`, a
  `ForwardChange` (linked to the existing smoke `ForwardSource`), and a
  `ForwardChangePolicyRule` (linked to the new policy).
- Add all three to the `fixtures` dict so their detail routes
  (changelog/delete/edit/journal/review) are enumerated and probed too, not
  just their list pages.

## Validation

Ran `scripts/validate_installed_routes.py` directly against a live dev
container (`docker exec ... python3 scripts/validate_installed_routes.py`):
before the fix, `_require_menu_coverage()` refused with exactly the reported
missing-routes message; after adding the two `MENU_ROUTES` entries alone, a
new gap surfaced (`forwardchangepolicyrule_changelog` returned 404, from
`fixture_model_for` matching the policy-rule's routes to the wrong, shorter
prefix `forwardchangepolicy`); after adding the policy-rule fixture and dict
entry, every menu and detail route in the plugin returned 200/302/405 as
expected. Fixture rows created for this manual verification were deleted
afterward so they do not linger in the dev database.

## Rollback

Revert the single file. No migrations, no behavior change outside this
probe script.

## Decision Log

- **2026-09-17** -- Found while validating an unrelated change (forward-sdk
  migration step 2) whose own `invoke ci` run failed here; fixed and landed
  separately rather than bundled into that migration PR, since this gap
  predates it and is unrelated to the client extraction.
