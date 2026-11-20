# Browser UAT as the UI gate

## Goal

Every UI check in the repo goes through agent-browser, as named journeys, and the
release gate fails if a required journey does not run and pass. Prompted by 3.0.18: the
site rules tag dropdown shipped broken because no check opened a tag row in a real
NetBox page (NetBox's dropdown enhancer copied the options at load, so Forward's tags
never appeared).

## Constraints

- A change must not loosen its own gate: the old 15 checks and 8 screenshots all remain
  (the journeys are the old harness split by area); this adds checks and removes none.
  `scripts/ui/required-journeys.txt` lists what the gate requires; shrinking it is a
  gate change and must be reviewed on its own.
- No production code branches on the UI-test environment. The seed command stays
  guarded by `FORWARD_UI_HARNESS_ISOLATED`.
- No customer names, ids or data in fixtures (the stub generates its own).

## Touched Surfaces

- `scripts/forward_ui_harness.mjs` (runner), `scripts/ui/lib.mjs`, `scripts/ui/journeys/`,
  `scripts/ui/required-journeys.txt`, `scripts/ui/forward_stub.py`,
  `forward_netbox/management/commands/forward_seed_ui_harness.py`,
  `scripts/tests/test_ui_gate.py`.
- Also in this tranche: `widgets.py` (the dropdown fix), `change_control/predict.py` and
  `workspace.py` (Forward 26.11 predicted snapshots), forward-sdk 0.1.40.

## Approach

A stdlib Forward stand-in runs in the NetBox container, so the source form, its pickers
and a save reach the real client. Journeys cover the rules table, save, reload, refusal,
edit, clear, and the support bundle. The runner fails a journey that records no check or
whose page throws, and fails the run if a required journey did not run and pass.

## Validation

- `invoke ui-test`: 13 journeys. Broken on purpose: with the dropdown fix removed the two
  rules journeys fail; with an injected page error only that journey fails.
- Predict: 85 tests; the new tests fail with the flag removed from the status check and
  from the SDK call.
- Not verified: against a live Forward (the stub stands in); Forward 26.11 itself.

## Rollback

Revert the tranche. The journeys are test-only; the dropdown and Predict changes are
small and independent.

## Decision Log

- Stub in the NetBox container rather than browser-level route mocks, so the real client
  and the real save path run.
- Required journeys are listed in a file the runner reads, so deleting a journey fails
  the gate instead of quietly shrinking it.
