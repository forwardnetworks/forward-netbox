# Say why the "none chosen" devices have no primary IP, in the bundle

## Goal

A support bundle showed 408 devices under "Has an address, none chosen" while the sync's own management-address summary said 585 devices share their address with another device. The finding sent the operator to the job log for a number the bundle already held. Put the reason in the finding and the bundle, so no one has to ask the customer.

## Constraints

- Counts and device ids only in the bundle; no names.
- No claim about what a sync will do; the text reports what the last management-address step said.

## Touched Surfaces

- `forward_netbox/utilities/primary_ip_reasons.py`, `explained_findings.py`, `views.py` (bundle payload).
- `forward_netbox/tests/test_explained_findings.py`; `docs/01_User_Guide/troubleshooting.md`.

## Approach

1. The unchosen-address reason carries the management-address step's reported counts as a cross-reference.
2. The "already on" advice stops pointing at the job log and says what a Mgmt_ device tag does.
3. The bundle lists a few device ids per reason, from one classification shared with the reasons.

## Validation

- `invoke test-isolated` on `test_explained_findings` and `test_bundle_triage_diagnostics` (67 tests).
- `invoke harness-check`, `pre-commit run --all-files`, then the pre-push `invoke ci`.

## Rollback

Revert; the finding returns to the previous wording and classification.

## Decision Log

- The classifier is unchanged: broadening "shared" to any holder was reviewed out, because the sync-time step does not skip on sharing and uniqueness is per address record, so it could hide fixable devices.
- The summary counts are shown as a sync-wide cross-reference, not as a split of this reason's devices, because the step counts every bare device and the classifier counts by what NetBox holds.
