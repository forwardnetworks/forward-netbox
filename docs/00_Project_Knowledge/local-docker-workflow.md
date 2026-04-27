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

## Reset Guidance

Use Docker resets only for local validation. Do not encode tenant-specific network IDs, snapshot IDs, credentials, or customer-derived names in committed tests or docs.

When validating Branching behavior, remove stale local branches and ingestions before rerunning the same sync name. Branch names are unique, so stale branch artifacts can mask the behavior under test.
