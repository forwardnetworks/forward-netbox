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

For high-volume local ingestion validation, tune the Docker stack to host capacity first.

```bash
invoke optimize-runtime --worker-replicas 0 --query-fetch-concurrency 16 --nqe-page-size 10000 --apply-postgres
```

`--worker-replicas 0` means "auto-size from host CPU count". Set `--source-name <name>` to persist `query_fetch_concurrency` and `nqe_page_size` on a specific `ForwardSource`.

### Optional: Pin Postgres Data To A Separate NVMe Path

By default, Postgres data uses the Docker-managed named volume (`netbox-postgres-data`).
If you need to isolate Postgres IO onto a separate device (for example `/dev/nvme3n1` mounted at `/var/lib/container-storage`), set `FORWARD_NETBOX_POSTGRES_DATA_PATH` to an absolute host path.

`development/docker-compose.yml` supports this directly:

- default: `netbox-postgres-data` (named volume)
- override: `/absolute/host/path` (bind mount)

Example migration:

```bash
invoke stop
mkdir -p /var/lib/container-storage/forward-netbox/postgres
docker run --rm \
  -v forward-netbox_netbox-postgres-data:/from \
  -v /var/lib/container-storage/forward-netbox/postgres:/to \
  alpine sh -lc 'cp -a /from/. /to/'
echo 'FORWARD_NETBOX_POSTGRES_DATA_PATH=/var/lib/container-storage/forward-netbox/postgres' >> development/.env
invoke start
```

Verify:

```bash
docker compose --project-name forward-netbox --project-directory development \
  exec -T postgres sh -lc 'df -h /var/lib/postgresql/data'
```

Use `invoke smoke-sync` for a controlled sync smoke test once a source/sync is configured in the local NetBox instance.

```bash
invoke smoke-sync --plan-only
invoke smoke-sync --max-changes-per-branch 1000
```

Use targeted ingestion/delete regressions to validate native full-import and diff-delete behavior before live customer reruns.

```bash
invoke ingestion-delete-regression
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
