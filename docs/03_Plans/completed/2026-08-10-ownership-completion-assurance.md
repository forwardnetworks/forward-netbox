# Prove the ownership domain completes, not just that a helper returns

## Goal

Be sure 2.7.5 actually fixed the customer's failure, rather than sure that a
helper behaves.

## Contract

- The assertion is on the recorded DOMAIN status, because that is what the
  Drift Report reads and what "Ownership: Incomplete" reflects.
- The tests must fail against pre-2.7.5 behaviour, or they prove nothing.

## Constraints

- `_apply_maintained_device_tag` takes keyword-only arguments after
  `device_names`.
- Tag ASSIGNMENT removal is gated on `_domain_is_current` (ownership.py:637),
  which compares against a promoted baseline. A fixture that never promotes one
  will not see removal, and that is correct - the gate stops one sync stripping
  a tag another sync still claims.

## Touched Surfaces

- `forward_netbox/tests/test_ownership_completes_with_preexisting_status_tag.py`

## Approach

Drive the reconciliation the scope-tag job calls, with `forward-backfilled`
already present and unowned - the customer's exact state - and assert the
STATUS_TAGS domain reaches COMPLETED, repeatedly, while an operator's own
assignment survives.

## Validation

- 4 tests, OK; full plugin suite 2014 tests, OK (4 skipped)
- All four confirmed to FAIL with the pre-2.7.5 refusal restored

## Rollback

Revert. Coverage returns to the isolated helper test.

## Decision Log

- **The existing coverage was not evidence.** `_ensure_managed_tag` passing in
  isolation says nothing about whether the domain completes, and the domain is
  the thing the customer sees. Asserting the helper was assurance theatre.
- **Verified the tests fail against the old behaviour.** Restored the refusal
  and confirmed all four error, then restored the fix. A regression test never
  observed failing is a guess.
- **Corrected one expectation rather than the code.** The fourth test first
  asserted the tag assignment was removed; it is not, because the fixture never
  promotes a baseline and removal is gated on domain currency. The gate is
  right, so the test now asserts the claim release and documents why the
  assignment is out of scope here.

## Open

- Whether the customer's specific conflict was this one is still unconfirmed -
  their tag table was never inspected. What is now proven is that this failure
  mode is fixed end to end, and that a surviving conflict names itself.
