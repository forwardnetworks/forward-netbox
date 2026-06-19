# Operations Guide

Day-to-day operator workflows on the Forward sync detail page, plus the
equivalent management commands. All of the heavy actions run as **background
jobs** — the button queues a job and returns immediately; watch the sync's
**Jobs** tab for the result.

## Snapshot selectors

Each sync resolves which Forward snapshot to read via its **Snapshot Selection**:

- `latestProcessed` — the most recent fully-processed snapshot.
- `latestCollected` — the most recent snapshot in which the in-scope devices were
  actually collected, skipping snapshots where collection was canceled and data
  was backfilled. Use this when a routine snapshot occasionally backfills your
  devices.
- A specific snapshot id — pin to one snapshot.

Note: `latestProcessed` and `latestCollected` often resolve to the **same**
snapshot. When the latest snapshot itself backfilled some in-scope devices,
neither selector hides them — see *Backfilled devices* below.

## Device scope reconciliation

The **Scope Reconciliation** page compares the devices NetBox holds against the
sync's Forward tag scope and reports:

- **In scope (collected)** — tagged devices freshly collected this snapshot.
- **Tagged but backfilled** — tagged devices whose data was backfilled (carried
  over, not freshly collected) in this snapshot. Not errors; see below.
- **Out of scope (orphans)** — NetBox devices not in the Forward scope at all
  (usually left by an earlier, broader sync).
- **In scope, missing from NetBox** — collected devices not yet synced in.

CLI equivalent:

```
python manage.py forward_device_scope_reconciliation_audit --sync-name "<sync>"
```

### Prune orphans

The **Prune orphans** button queues a job that deletes the out-of-scope devices
(and their interfaces/IPs). It refuses to run if the Forward scope query returns
zero devices (which would treat everything as an orphan), and it preserves
tagged-but-backfilled devices. CLI: add `--prune-orphans` (dry run) then
`--apply`.

### Backfilled devices

Backfilled devices are tagged, in scope, and real — Forward just did not collect
them in this snapshot and carried over older data. A persistent backfilled set
usually means a **collection gap in Forward** (unreachable device, canceled
collection), not a plugin problem.

The **Tag backfilled devices** button queues a job that applies a maintained
`forward-backfilled` tag to them. Filter `/dcim/devices/?tag=forward-backfilled`
to see the list. The tag self-heals: a device that collects fresh again loses the
tag on the next run.

The **Collection gap** health signal (sync health summary) flags when the
backfilled count is non-trivial so you can investigate collection in Forward.

## Module readiness

NetBox Branching cannot create a new device's module bays during a merge, so
optional `dcim.module` sync needs the bays to exist first. The **Module
Readiness** page reports how many module rows already have bays, how many are
missing, and whether sync is ready.

- **Ready** reflects *missing module bays only*. Module rows for devices not in
  NetBox skip harmlessly and do not hold readiness at "No".
- The **Create missing module bays** button queues a job that creates the bays
  directly (MPTT-safe, idempotent). Re-run the sync afterward to import modules.

When modules are created, the device's existing Forward-synced interfaces are
**adopted** into the module rather than recreated, so enabling module sync does
not collide with the interface sync.

CLI equivalent: `python manage.py forward_module_readiness --sync-name "<sync>"`.

## Dependency preview

The **Preview Dependencies** button queues a job that builds the multi-branch
dependency plan (a heavy live dry-run). When it finishes, **View Last Preview**
renders the cached result and `?format=json` downloads it. The preview never runs
the dry-run in the web request, so it does not time out on large fabrics.

## Running a sync

- **Run** / **Adhoc Ingestion** enqueues the sync job.
- **Validate** runs query validation without applying.
- **Export Support Bundle** / **Export ZIP** collects diagnostics (live source
  health, query drift, data-file checks) for support.

## Apply engine

Models apply through either the per-row **adapter** or the batched **bulk-ORM**
engine. The default safe set (all built-in models as of 1.5.10) runs bulk-ORM;
a handful of relationship-heavy models stay on the adapter. See
[Apply Engine Model Matrix](../02_Reference/apply-engine-model-matrix.md).

## Releasing the plugin

Maintainers cut releases with `invoke release` (see `scripts/release.py`):

```
invoke release --version X.Y.Z --summary "one-line note" --write
```

`--write` runs prepare (version bump + compatibility tables) and the local CI
mirror. Rollout (branch, push, tag, GitHub release, PyPI) only happens with
`--publish`/`--finish`, after GitHub CI is green.
