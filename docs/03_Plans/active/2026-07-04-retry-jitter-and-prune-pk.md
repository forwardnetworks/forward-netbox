# Retry jitter + Retry-After, and PK-anchored device prune

**Date:** 2026-07-04

## Goal
Two reliability/safety hardening items from the enterprise assessment:

1. The Forward API retry backoff was linear, had no jitter, and ignored the
   `Retry-After` header — so concurrent workers could thundering-herd a throttled
   endpoint and a 429 was retried on a fixed schedule regardless of server advice.
2. The device prune deleted by `name__in` per batch; NetBox device names are not
   globally unique, so re-matching by name at delete time is fragile.

## Constraints
- No change to which devices are pruned (scope is still name-keyed) — only the
  delete is anchored to explicit PKs.
- Cap any single retry wait so a hostile/large `Retry-After` cannot stall a sync.
- No schema/migration change.

## Touched Surfaces
- `forward_netbox/utilities/forward_api_impl.py` — `_parse_retry_after` (delta-
  seconds only; HTTP-date form falls back to backoff) and `_retry_wait_seconds`
  (honor Retry-After else linear backoff, plus additive jitter in `[0, base]`,
  capped at `MAX_FORWARD_API_RETRY_BACKOFF_SECONDS = 60`). The `_request` retry
  loop captures `Retry-After` on transient 429/503 and uses the helper.
- `forward_netbox/utilities/scope_reconciliation.py` — `prune_orphan_devices`
  resolves the out-of-scope names to PKs once, then deletes by `pk__in`.
- Tests: `test_forward_api.py::RetryBackoffHelperTest` (parse + wait bounds + cap).

## Approach
Small, self-contained helpers so the retry math is unit-testable by bounds without
mocking `random`. The prune change is behavior-preserving (same device set) but
anchors the delete to rows identified at planning time.

## Validation
Full suite on 4.6.4; new retry-helper tests; scope-reconciliation tests;
lint/harness.

## Rollback
Revert the commit; both changes are localized and carry no schema impact.

## Decision Log
- Additive jitter `[0, base]` on top of linear backoff (not full-jitter
  exponential): keeps the existing, well-understood cadence while breaking
  lockstep — minimal behavior change.
- Honor only the delta-seconds `Retry-After` form; ignore HTTP-date to avoid
  clock-skew surprises, falling back to backoff.
- Prune stays name-keyed for *scope* (making scope site/identity-aware is a
  separate design item); only the delete is PK-anchored.

## Bundled changes
- Jittered, Retry-After-aware, capped API retry backoff.
- Device prune deletes by resolved PK instead of re-matching by non-unique name.
