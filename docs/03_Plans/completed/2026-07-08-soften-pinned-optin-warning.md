# Reword the pinned-query opt-in Health warning (2.4.3)

## Goal

Stop the 2.4.1 pinned-query opt-in warning from reading as a confirmed failure.
It fires on any `query_id` map with the feature enabled — including one that is
already current after publishing — and its wording ("silently ignored, so
nothing new syncs") led a design partner to think the fix hadn't worked.

## Constraints

- Wording-only; no change to when the warning fires or to any sync behavior.
- Must keep the badge label in sync with the elevated status (the 2.4.1 bug).
- Point operators at the check that CAN verify (Export Live Query Drift).

## Touched Surfaces

- `forward_netbox/utilities/health.py` (`_elevate_optin_pinned_query_drift` message/remediation/label)
- `forward_netbox/utilities/query_binding_resolution.py` (`_QUERY_DRIFT_STATUS_LABELS` badge)
- `forward_netbox/tests/test_endpoints_import.py` (assert new wording)

## Approach

- Badge label `direct_query_id_optin_stale_risk` → "Pinned — can't verify
  locally".
- Message: state the Health page can't inspect a pinned query locally; frame the
  staleness as a possibility ("may predate the feature"), not a fact.
- Remediation: verify with Export Live Query Drift (`source_matches_bundled`)
  first; only if it mismatches, Publish Bundled Queries → Refresh Query IDs →
  re-sync.

## Validation

`test_endpoints_import` asserts the reworded label/message/remediation and that
the badge tracks the elevated status. Full suite + lint + harness green.

## Rollback

Revert the branch — pure wording + test; no data or behavior change.

## Decision Log

- Keep severity `warn` (still a legitimate unverifiable-config heads-up) rather
  than downgrading to `info`, which would hide the genuinely-stale case again.
  The local summary intentionally makes no live call; the fix is honest wording
  plus a pointer to the on-demand live check.
