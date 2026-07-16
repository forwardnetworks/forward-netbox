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

For long ingestion tests, disable worker autoreload before starting the stack so
ordinary file edits do not restart active RQ workers:

```bash
echo 'FORWARD_NETBOX_WORKER_AUTORELOAD=0' >> development/.env
invoke restart
```

Leave the default autoreload enabled for normal development.

For field-scale runs, treat the local stack as a dedicated ingestion runtime:

- Run `invoke optimize-runtime` before the smoke or scale run.
- Keep `nqe_page_size` at `10000` unless Forward API or NetBox worker telemetry
  shows pressure.
- Use `query_fetch_concurrency=16` only when workers and PostgreSQL have
  headroom; otherwise start at `6` to `12`.
- Keep `Use safe bulk ORM models` enabled on the sync for the parity-tested safe
  set. Newly created syncs enable it by default; existing syncs preserve their
  stored setting.
- `invoke smoke-sync` also enables the safe bulk ORM model set by default; use
  `--enable-bulk-orm=False` only for adapter-only comparison evidence.
- Enable `Stage next shard during merge` only when auto-merge is enabled and
  support evidence shows queue/merge wait dominates while the database still
  has headroom.
- Put Postgres and fetch artifacts on fast local storage when available.

Do not run Django test tasks against this same shared runtime while a Forward
execution run is queued, running, or waiting. The test suite can touch RQ
registries and test database state; sharing it with a live ingestion can move a
real job into failed/abandoned state. `invoke test`, `invoke scenario-test`, and
`invoke ingestion-delete-regression` fail fast when active execution runs are
detected. Set `FORWARD_NETBOX_ALLOW_SHARED_RUNTIME_TESTS=1` only when you
intentionally want to bypass that guard.

CI-style gates such as `invoke test-ci`, `invoke scenario-test-ci`, and
`invoke scale-chaos-test` use the shared runtime only when the active-run guard
can inspect it and finds no active run. `invoke playwright-test` uses the same
guard for the deterministic UI harness. If the guard detects an active run or
cannot inspect the shared runtime, for example because local Postgres is already
at its connection limit, the task runs against an isolated compose project
instead of bypassing the shared-runtime safety check.

Use the isolated test runtime when a live ingestion is active or when you want a
repeatable full regression lane that does not share RQ, Redis, or Postgres with
the UI harness:

```bash
invoke test-isolated
invoke test-isolated --test-label forward_netbox.tests.test_sync
```

`test-isolated` uses the separate compose project `forward-netbox-test` and runs
NetBox tests through a one-off container, so it does not publish the web port or
touch the primary `forward-netbox` runtime. It keeps the isolated Postgres volume
by default so later `--keepdb` test runs are faster. Add
`--no-keep-runtime` to remove the isolated containers and volumes after the
run.

The Docker build context is intentionally pruned by `.dockerignore`. Keep large
local artifacts such as `development/logs/`, virtualenvs, `site/`, `dist/`,
`node_modules/`, and Playwright output out of the image context; otherwise the
first isolated test run spends most of its time uploading irrelevant files to
the Docker builder.

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

`invoke runtime-capacity-review --source-name <name>` also records the Docker
root, Postgres data mount, source fetch settings, worker count, and PostgreSQL
tuning recommendations in a JSON artifact. Use it before long local tests so the
evidence shows whether the run is using the intended storage and worker profile.

Fetch artifacts are separate from Postgres data. For large local runs, point
them at fast scratch storage rather than durable shared storage:

```bash
echo 'FORWARD_NETBOX_FETCH_ARTIFACT_HOST_PATH=/var/lib/container-storage/forward-netbox/fetch-artifacts' >> development/.env
```

The compose runtime mounts this host path into the NetBox and worker containers
as `/fetch-artifacts` and sets `FORWARD_NETBOX_FETCH_ARTIFACT_DIR`
automatically. If no host path is configured, compose uses
`development/fetch-artifacts`, which is git-ignored.

The artifact directory is runtime scratch space for retry/resume support; do not
commit it or back it up with customer data. The local compose default allows
artifacts up to `512 MiB` so large full-model fallback artifacts can be reused
within a run instead of silently falling back to the smaller library default.

Use `invoke smoke-sync` for a controlled sync smoke test once a source/sync is configured in the local NetBox instance.

```bash
invoke smoke-sync --plan-only
invoke smoke-sync --max-changes-per-branch 1000
```

Use targeted ingestion/delete regressions to validate native full-import and diff-delete behavior before live customer reruns.

```bash
invoke ingestion-delete-regression
invoke sync-health-gate --sync-id 50 --max-polls 120 --interval-seconds 30
invoke sync-health-gate --sync-id 50 --max-polls 10 --interval-seconds 30 --allow-nonterminal
invoke sync-health-monitor --sync-ids 50,51 --max-polls 6 --interval-seconds 30 --allow-nonterminal --output-json docs/03_Plans/evidence/sync-health-monitor.json
```

`sync-health-gate` is the recommended pre-release dataset gate: it combines
`forward_watch_sync`, `forward_blocker_audit`, and `forward_warning_audit`
and exits nonzero on any blocker/warning/error regression.

`sync-health-monitor` uses the same audits across multiple sync IDs in one loop
and writes timestamped evidence to JSON. Use it when two lanes are running in
parallel (for example, an A/B or recovery replay) and you need continuous proof
that no blocker/warning/error findings appeared during the soak window.

For release readiness on the release-validation dataset, label and enforce the
field-scale artifact. The workflow selects an existing configured Forward Source
without putting credentials or source identifiers in the command or artifact:

```bash
export FORWARD_SMOKE_DATASET_LABEL=release-smoke
invoke release-runtime-preflight --dataset-label=release-smoke
invoke field-scale-runtime-matrix --resume=False
invoke release-dataset-gate --dataset-label=release-smoke
invoke release-readiness-audit --dataset-label=release-smoke
```

Use `invoke playwright-test` for the deterministic UI harness. It applies pending
Django migrations, seeds synthetic Forward records in the Docker NetBox container,
logs in through the browser, visits the sync and ingestion workflow pages, and
writes local screenshots plus a JSON summary under `.playwright-artifacts/`.
Set `PLAYWRIGHT_SKIP_MIGRATE=true` only when the target database has already been
migrated by the caller, as in GitHub CI.
When the shared runtime has an active execution run or cannot be inspected, the
task brings up the temporary `forward-netbox-ui-test` compose project on port
`18080`; override the port with `FORWARD_NETBOX_PLAYWRIGHT_HOST_PORT` if needed.

```bash
npm ci
npx playwright install chromium
invoke playwright-test
```

## Reset Guidance

Use Docker resets only for local validation. Do not encode tenant-specific network IDs, snapshot IDs, credentials, or customer-derived names in committed tests or docs.

When validating Branching behavior, remove stale local branches and ingestions before rerunning the same sync name. Branch names are unique, so stale branch artifacts can mask the behavior under test.
