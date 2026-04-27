# Local Docker Workflow

The development stack runs NetBox, Postgres, Redis, and workers through `development/docker-compose.yml`.

## Common Commands

```bash
invoke build
invoke start
invoke check
invoke test
invoke stop
```

Use `invoke smoke-sync` for a controlled sync smoke test once a source/sync is configured in the local NetBox instance.

```bash
invoke smoke-sync --plan-only
invoke smoke-sync --max-changes-per-branch 1000
```

Use `invoke playwright-test` for the deterministic UI harness. It applies pending
Django migrations, seeds synthetic Forward records in the Docker NetBox container,
logs in through the browser, visits the sync and ingestion workflow pages, and
writes local screenshots plus a JSON summary under `.playwright-artifacts/`.
Set `PLAYWRIGHT_SKIP_MIGRATE=true` only when the target database has already been
migrated by the caller, as in GitHub CI.

```bash
npm ci
npx playwright install chromium
invoke playwright-test
```

## Reset Guidance

Use Docker resets only for local validation. Do not encode tenant-specific network IDs, snapshot IDs, credentials, or customer-derived names in committed tests or docs.

When validating Branching behavior, remove stale local branches and ingestions before rerunning the same sync name. Branch names are unique, so stale branch artifacts can mask the behavior under test.
