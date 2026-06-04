# Idempotence And Identity Hardening

## Goal

Turn the prefix VRF churn fix into a broader release-hardening gate by making
model identity and repeat-sync no-op behavior easier to prove before future
releases.

## Constraints

- Keep NQE as the source of truth for row shape and normalization.
- Do not broaden the 1.2.1 prefix fix into generic null-preserving coalesce
  behavior without model-specific evidence.
- Do not store customer rows, network IDs, query IDs, screenshots, or tenant
  labels in tests or committed evidence.
- Keep this tranche additive: tests and contract guardrails first, no broad
  adapter refactor.

## Touched Surfaces

- `forward_netbox/tests/test_sync.py`
- `forward_netbox/tests/test_sync_runner_contracts.py`
- `docs/00_Project_Knowledge/validation-matrix.md`

## Approach

1. Add explicit tests for nullable scoped identity contracts so default-VRF
   behavior is deliberate per model instead of accidental fallback behavior.
2. Add repeat-apply no-op tests for representative model families beyond
   `ipam.prefix`, asserting that unchanged rows do not create additional
   `ObjectChange` rows or SQL `UPDATE` statements.
3. Document the new idempotence gate as a release-hardening validation item.

## Validation

- Passed: focused isolated Django labels for sync contract tests and new
  repeat-sync no-op adapter checks.
- Passed: `invoke harness-check`
- Passed: `invoke harness-test`
- Passed: `invoke lint`
- Passed: `invoke check`
- Passed: `invoke docs`
- Passed: `invoke test-isolated --test-label='forward_netbox.tests.test_sync_runner_contracts forward_netbox.tests.test_sync'`
  - 343 tests

## Rollback

Remove the added tests/docs and restore the previous validation matrix wording.
Because this tranche does not change production behavior, rollback does not
require NetBox data migration or cleanup.

## Decision Log

- Chosen: contract and repeat-apply tests first because they catch the bug
  class that caused prefix VRF churn without changing runtime behavior.
- Chosen: assert both absence of SQL `UPDATE` statements and stable
  `ObjectChange` counts for representative adapter paths, because object counts
  alone would miss silent churn.
- Rejected: generic null-preserving coalesce logic for all models because prior
  routing/IPAM fixes show nullable scope needs model-specific evidence.
