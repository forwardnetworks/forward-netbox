# Fast Bootstrap Change Tracking

## Goal

Make fast bootstrap visibly behave like a real NetBox write path: native change
logging should exist, ingestion counters should update from actual object
changes, and operators should not mistake a direct-write baseline for a dry run.

## Constraints

- Keep NQE and the existing row adapters as the source of truth.
- Keep fast bootstrap branchless; do not create review branches or Branching
  `ChangeDiff` rows for this backend.
- Use native NetBox request/event tracking and `ObjectChange` records for audit.
- Preserve Branching-backed ingestion behavior unchanged.
- Treat transient Forward API gateway/timeouts as retryable without hiding
  validation failures or non-transient HTTP errors.

## Touched Surfaces

- `forward_netbox/utilities/fast_bootstrap_executor.py`
- `forward_netbox/utilities/direct_changes.py`
- `forward_netbox/utilities/forward_api.py`
- `forward_netbox/models.py`
- `forward_netbox/migrations/0011_forwardingestion_change_request_id.py`
- `forward_netbox/views.py`
- `forward_netbox/tables.py`
- `forward_netbox/filtersets.py`
- Fast-bootstrap and Forward API tests
- User guide, troubleshooting, and README documentation

## Approach

- Run fast-bootstrap writes inside NetBox request/event tracking.
- Store the direct-write request id on `ForwardIngestion`.
- Derive fast-bootstrap created/updated/deleted/applied counters from native
  `ObjectChange` rows for the sync's enabled NetBox models.
- Show branchless ingestion changes from `ObjectChange` rows while preserving
  Branching `ChangeDiff` display for branch-backed ingestions.
- Retry transient Forward HTTP responses (`408`, `429`, `502`, `503`, `504`)
  using the existing source retry/backoff setting.

## Validation

- Targeted fast-bootstrap tests prove direct NetBox writes create native
  `ObjectChange` rows and update ingestion counters.
- Targeted Forward API tests prove transient HTTP 504 responses retry and then
  raise a connectivity error after retry exhaustion.
- `manage.py check` passes in the local NetBox container.
- Run harness, docs, lint, check, and whitespace validation before release.

## Rollback

Revert the migration/model field plus fast-bootstrap request tracking changes.
Existing fast-bootstrap ingestions with a stored request id can keep the nullable
value safely, but without the view/helper changes their direct-write changes will
again be visible only through the global NetBox change log.

## Decision Log

- Rejected synthetic Branching diffs for fast bootstrap because the backend is
  intentionally branchless and native NetBox `ObjectChange` is the correct audit
  primitive for direct writes.
- Rejected Python-side row counters as the authoritative created/updated/deleted
  source because they do not prove NetBox persisted an object change.
