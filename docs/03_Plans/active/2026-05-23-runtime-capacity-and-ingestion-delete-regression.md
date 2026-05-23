# 2026-05-23 Runtime Capacity And Ingestion/Delete Regression

## Goal

Codify local NetBox runtime tuning for high-volume sync testing and add durable automated regression coverage for full ingestion + deletion behavior so large-run validation is not dependent on manual operator repro.

## Constraints

- Keep native NetBox/Branching workflow intact; no alternate ingestion control plane.
- No customer IDs, credentials, snapshots, or tenant labels in committed artifacts.
- Keep new regression deterministic and CI-safe (no live customer dependency).

## Touched Surfaces

- `tasks.py`
- `scripts/tests/test_tasks.py`
- `forward_netbox/tests/test_synthetic_scenarios.py`
- `development/docker-compose.yml`
- `docs/00_Project_Knowledge/local-docker-workflow.md`
- `docs/01_User_Guide/configuration.md`
- `docs/01_User_Guide/troubleshooting.md`

## Approach

- Add invoke tasking for host-capacity runtime optimization:
  - NetBox worker scaling
  - Postgres runtime tuning for local Docker validation
  - optional Forward source fetch knob update (`query_fetch_concurrency`, `nqe_page_size`)
- Add invoke regression task for ingestion/delete coverage.
- Add synthetic regression that executes full ingest then diff-delete through the native runner path.
- Document practical large-ingestion tuning guidance in user docs.
- Add optional Docker Compose Postgres data path override so local operators can pin Postgres onto a dedicated NVMe mount when needed.

## Validation

- `python -m unittest discover -s scripts/tests -p 'test_*.py'`
- `docker compose --project-name forward-netbox --project-directory development exec -T netbox bash -lc "cd /opt/netbox/netbox && python manage.py test --keepdb --noinput forward_netbox.tests.test_synthetic_scenarios.SyntheticSyncScenarioHarnessTest.test_full_site_ingestion_then_diff_delete"`
- `poetry run invoke ingestion-delete-regression`
- `poetry run invoke harness-check`
- `poetry run invoke docs`

## Rollback

- Revert task additions and docs updates in `tasks.py`, `scripts/tests/test_tasks.py`, and user docs.
- Remove new synthetic regression if it proves flaky in CI and replace with narrowed deterministic assertions.

## Decision Log

- Capacity tuning task is intentionally local/runtime-facing and does not change production plugin defaults.
- Chose an optional Compose bind-mount override for Postgres data instead of forcing a migration. Default behavior stays unchanged; operators can migrate only when IO isolation is needed.
