# Fix: warn when opt-in features hit stale pinned Forward queries + release 2.4.1

**Date:** 2026-07-07

## Goal
A design partner enabled **Import SNMP Endpoints as Devices** (`sync_endpoints`)
and **Sync Device Tags** (`sync_device_tags`) but saw no Avocent endpoints and no
`Mgmt_*` tags after upgrading to 2.4.0. Root cause: the source runs **org-managed
pinned Forward query IDs** (`direct_query_id_unverified`). The plugin injects the
opt-in parameters correctly (verified: the `query_id` execution path passes
`parameters` through to `client.run_nqe_query`), but the pinned org queries
predate the feature, so Forward runs the old query text and silently ignores the
new parameter. The plugin flagged this only as a quiet `info` badge, so the
failure was invisible. Make it loud + name the remediation.

## Constraints
- No NQE / apply / model change. Health-display + label only.
- Keep `sync_health_summary` local (no live Forward calls on page render); the
  signal must be derivable from local state (source parameters + map mode).
- Do not silently override pinning or auto-mutate the org — pinned direct query
  IDs are an intentional, operator-governed reproducibility choice.

## Touched Surfaces
- `forward_netbox/utilities/health.py` — `_elevate_optin_pinned_query_drift()`
  post-processes local drift: when an opt-in feature is enabled on the source and
  its backing bundled query (`forward_devices.nqe` → `sync_endpoints`,
  `forward_device_feature_tags.nqe` → `sync_device_tags`) runs a pinned direct
  query ID, elevate that map from `info` → `warn` with a publish + Refresh Query
  IDs remediation.
- `forward_netbox/utilities/query_binding_resolution.py` — display label for the
  new `direct_query_id_optin_stale_risk` status.
- `forward_netbox/tests/test_endpoints_import.py` — 5 unit tests for the
  elevation (enabled/disabled × endpoints/tags, plus non-pinned untouched).
- `pyproject.toml`, `forward_netbox/__init__.py`, three README tables,
  `CHANGELOG.md` — version bump to 2.4.1.

## Approach
The overall query-drift health check is severity-driven
(`query_drift_check_status`: any `warn` → `warn`), so flipping the affected
item's severity is sufficient to surface it. Elevation only fires when the
operator has actually turned the feature on (`sync_endpoints` truthy, or
`sync_device_tags` non-empty) AND the backing map is pinned — current-but-pinned
setups without the feature stay quiet. The badge text tells the operator exactly
how to make the setting take effect.

## Validation
`test_endpoints_import` (10 tests incl. the 5 new); `test_health` +
`test_query_binding` (57, regression on the drift status change); full Django
suite (970, OK skipped=28); harness + sensitive; `gen_changelog --check`.

## Rollback
Revert the `health.py` helper + its call and the label entry; drift returns to
the prior silent `info` badge.

## Decision Log
- Warn, don't auto-fix. Publishing bundled queries to the org and rebinding
  pinned IDs is the operator's one-time action (Publish bundled queries with
  Overwrite → Refresh Query IDs → re-sync); the plugin surfaces it rather than
  silently running the bundled text or mutating the org behind the pinning.
- Local heuristic over live lookup: keeps page render offline; the honest framing
  is "we can't verify this pinned query is current — if the feature output is
  missing, here is the fix."
- Immediate partner remediation (independent of this code): republish the 2.4.x
  bundled queries to their Forward org folder, Refresh Query IDs, re-run sync.

## Bundled changes
Fix: opt-in features (SNMP endpoint import, device-tag sync) silently did nothing
on sources that run org-managed pinned Forward query IDs predating the feature —
the sync Health page now raises an actionable warning (publish the bundled
queries to Forward, then Refresh Query IDs) instead of a silent badge.
