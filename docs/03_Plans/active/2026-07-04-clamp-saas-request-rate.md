# Clamp the SaaS API request rate to a safe ceiling

**Date:** 2026-07-04

## Goal
Prevent an operator footgun flagged by the enterprise assessment: the source form
accepts `api_requests_per_minute` up to `MAX_FORWARD_API_REQUESTS_PER_MINUTE`
(60000), but Forward SaaS enforces a per-tenant rate ceiling. A high value on a
SaaS source can get the tenant throttled or blocked.

## Constraints
- SaaS only — custom/self-hosted deployments keep the full range.
- Clamp (silently reduce) rather than reject, to avoid breaking existing sources.
- No schema/migration change.

## Touched Surfaces
- `forward_netbox/utilities/model_validation.py` — `clean_forward_source` now
  clamps a SaaS source's `api_requests_per_minute` down to
  `DEFAULT_FORWARD_SAAS_API_REQUESTS_PER_MINUTE` (1800, the known-safe rate) when a
  larger value is configured.
- `forward_netbox/tests/test_models.py` — tests: SaaS clamps 60000 → 1800; a
  custom deployment keeps 6000.

## Approach
Reuse the existing SaaS branch and the `DEFAULT_FORWARD_SAAS_API_REQUESTS_PER_MINUTE`
constant; apply the clamp right after the general 0..MAX validation so the value is
already an int.

## Validation
Full suite 940 green (28 skip); the two new clamp tests; lint/harness.

## Rollback
Revert the commit; validation-only, no schema change.

## Decision Log
- Clamp to the safe DEFAULT (1800) rather than the ~2000 hard block: 1800 is the
  documented safe rate with headroom, and there is no separate SaaS-max constant.
- Clamp not reject: an existing SaaS source with a too-high value keeps working at
  the safe rate instead of failing validation on next save.

## Bundled changes
- SaaS sources can no longer be configured above the safe Forward request rate.
