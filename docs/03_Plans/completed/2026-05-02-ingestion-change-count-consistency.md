# Ingestion Change Count Consistency

## Goal

Keep ingestion change counts stable in UI after branch merge/cleanup so list/detail values remain consistent with merge logs.

## Constraints

- Preserve existing branch cleanup behavior.
- Do not require live branch `ChangeDiff` rows for historical ingestion reporting.
- Keep API/UI behavior backward compatible.

## Touched Surfaces

- `forward_netbox/models.py`
- `forward_netbox/views.py`
- `forward_netbox/utilities/merge.py`
- `forward_netbox/migrations/0007_forwardingestion_persisted_change_counters.py`
- `forward_netbox/tests/test_models.py`

## Approach

1. Add persisted ingestion counters for applied/failed/create/update/delete totals.
2. Write those counters at merge completion before branch cleanup.
3. Update annotated ingestion statistics to use persisted counters when branch is missing.
4. Add a regression test proving fallback behavior.

## Decision Log

- Chose persisted ingestion counters rather than recalculating from deleted branch rows.
- Chose branch-null conditional fallback in query annotations because `Count()` naturally returns `0`, making `Coalesce` insufficient.

## Validation

- `pre-commit run --files forward_netbox/models.py forward_netbox/views.py forward_netbox/utilities/merge.py forward_netbox/tests/test_models.py forward_netbox/migrations/0007_forwardingestion_persisted_change_counters.py`
- `docker compose --project-name forward-netbox --project-directory development exec -T netbox bash -lc "cd /opt/netbox/netbox && python manage.py migrate --noinput && python manage.py test --keepdb --noinput forward_netbox.tests.test_models.ForwardIngestionSnapshotSummaryTest.test_annotate_statistics_uses_persisted_counts_when_branch_missing"`
- `invoke harness-check`

## Rollback

- Drop persisted ingestion counter fields and migration.
- Remove merge-time counter persistence.
- Restore branch-only annotated statistics.
