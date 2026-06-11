# Merge Failure Visibility

## Goal

Make Branching merge failures operator-visible and leave branches in a terminal
failure state instead of a stale `Merging` state when a merge job errors. Carry
disabled async NQE client staging into the same release so future Forward 26.6
support is available behind explicit configuration.

## Constraints

- Do not convert real merge/apply failures into success.
- Do not retry ambiguous post-apply failures automatically.
- Preserve existing timeout and transient "not ready" retry behavior.
- Keep async NQE disabled by default and route only normal query execution
  through it when explicitly configured.
- Keep customer identifiers, screenshots, and live branch names out of tracked
  files.

## Touched Surfaces

- `forward_netbox/jobs.py`
- `forward_netbox/utilities/forward_api.py`
- `forward_netbox/utilities/forward_api_impl.py`
- `forward_netbox/tests/test_forward_api.py`
- `forward_netbox/tests/test_jobs.py`

## Approach

1. For non-retryable merge exceptions, write an error log entry before saving
   job data so the NetBox job page shows the reason.
2. If the Branch is still `Merging` when a non-retryable merge exception reaches
   the job handler, mark it `Failed`. This avoids an indefinite in-progress
   branch state after the RQ job is already errored.
3. Keep timeout and transient "not ready to merge" exceptions on their existing
   retry/recovery path.
4. Add Forward async NQE execution methods for the upcoming 26.6 API shape:
   submit execution, poll status, and page completed results.
5. Gate async NQE behind explicit client/source parameters so v1.3.4 preserves
   the synchronous NQE behavior used by current Forward SaaS.

## Validation

- `docker compose --project-name forward-netbox --project-directory development run --rm -T netbox bash -lc 'cd /opt/netbox/netbox && python manage.py test --keepdb --noinput forward_netbox.tests.test_forward_api.ForwardClientTest forward_netbox.tests.test_jobs.ForwardJobsTest'`
  - Passed: 96 tests.
- `invoke harness-check`
  - Passed.
- `invoke lint`
  - Passed.
- `invoke check`
  - Passed.
- `invoke harness-test`
  - Passed: 127 tests.
- `invoke scenario-test`
  - Passed: 53 tests.
- `invoke test`
  - Passed: 1000 tests before version bump, 1001 tests during final `invoke ci`.
- `invoke docs`
  - Passed.
- `invoke ci`
  - Passed after installing Node lockfile dependencies with `npm ci`; covered
    harness, lint, Docker build, NetBox system check, targeted/scenario/full
    Django tests, Playwright UI checks, docs, and package build for `1.3.4`.

## Rollback

Revert the job exception-handler changes and async NQE client additions.
Existing behavior returns, where generic merge exceptions may only be visible in
server logs, branches may remain `Merging`, and NQE execution remains
synchronous only.

## Decision Log

- Rejected treating merged-branch cleanup failure as success for this incident.
  Screenshots showed branches still in `Merging`, so the correct fix is the
  merge failure state machine and job-log visibility.
