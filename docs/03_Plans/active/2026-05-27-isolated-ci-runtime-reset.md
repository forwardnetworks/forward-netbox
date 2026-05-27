# Isolated CI Runtime Reset for Deterministic Test DB Creation

## Goal

Ensure isolated CI test runs always start from a clean compose project so `invoke ci` does not fail on stale `test_netbox` database state or locked sessions.

## Constraints

- Must preserve shared-runtime safety behavior: when active execution runs exist, Django tests must stay isolated.
- Must remain compatible with current NetBox 4.5.x local test stack and existing invoke task interfaces.
- Must keep pre-push and local `invoke ci` deterministic without requiring manual Docker cleanup.

## Touched Surfaces

- `tasks.py`
- `scripts/tests/test_tasks.py`

## Approach

1. In `_run_tests_in_isolated_runtime`, add a preflight `docker compose down --remove-orphans -v` for the isolated project before bringing up postgres/redis.
2. Keep isolated Django test invocation without `--keepdb` so test DB lifecycle is recreated each run.
3. Update task-unit tests to assert the new cleanup-first compose sequence.

## Validation

- `python -m pytest scripts/tests/test_tasks.py -q`
- `invoke ci`

## Rollback

- Revert `tasks.py` and `scripts/tests/test_tasks.py` changes from this plan.
- If needed, manually remove stale isolated resources:
  - `docker compose --project-name forward-netbox-test-ci --project-directory development down --remove-orphans -v`

## Decision Log

- Rejected using randomized isolated compose project names: it avoids collisions but increases runtime artifact churn and complicates troubleshooting.
- Chosen cleanup-first fixed project approach for deterministic behavior and easier operator debugging.
