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

### What scope prune removes

The tag scope is **device-derived**: a Forward row is treated as out-of-scope
(and deleted from NetBox when prune is enabled) only when it is tied to a device
that is not in the included-tag set. Coverage by model:

| Model group | Out-of-scope rows removed? |
| --- | --- |
| `dcim.device` and device-anchored children — `interface`, `macaddress`, `ipaddress`, `module`, `inventoryitem`, `cable`, `virtualchassis`, `extras.taggeditem`, device-scoped `ipam.fhrpgroup` | **Yes** — removed when their device is out of scope |
| `dcim.site` | **Yes** — a site with no in-scope device is removed |
| `ipam.prefix`, `ipam.vlan`, `ipam.vrf`, `dcim.manufacturer`, `dcim.platform`, `dcim.devicetype`, `dcim.devicerole` | **No** — network-global; imported and updated, never scope-deleted |

Network-global IPAM and metadata (prefixes, VLANs, VRFs, manufacturers,
platforms, device types, device roles) are **not** pruned by tag scope. They are
not owned by a device, so the scope filter has no signal to classify one as
out-of-scope, and deleting shared global objects is high blast radius — a /16
aggregate or a network-wide VLAN would look "unreferenced" by a scoped device
subset and be wrongly removed. Delete those manually in NetBox if a run truly
needs them gone.

To verify what a run removed, read the per-model **delete count** on the
ingestion page (the `delete_count` field in the support bundle): it lists exactly
which models had out-of-scope deletions.

### Auditing stale global IPAM

Because global IPAM is never scope-deleted, NetBox can accumulate prefixes, VLANs,
or VRFs that Forward no longer reports. The read-only audit lists them for manual
review — it reuses the apply engine's own identity matching, so a "stale" verdict
is exactly what the sync would consider the same object, and it **never deletes**:

```
python manage.py forward_scope_ipam_audit --sync-name "<sync>"
```

It prints, per model, `forward_rows`, `netbox_count`, `unmatchable_count` (objects
whose identity is indeterminate — never flagged), and `stale_count` with a
`stale_sample`. Restrict with `--models ipam.prefix,ipam.vlan,ipam.vrf`, size the
sample with `--limit`, and add `--fail-on-stale` for CI. Delete confirmed-stale
objects by hand in NetBox.

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

### Backfilled vs out of scope

These are different buckets — only one is removable:

- **Backfilled** (`forward-backfilled` tag) — the device **matches an included
  Forward tag** (it is in scope) but Forward could not freshly collect it in the
  latest snapshot (auth/timeout/incomplete setup), so older data carried over.
  These are **kept on purpose**; pruning a real device over a transient collection
  failure would be wrong. Scope membership is decided by the live Forward tag, not
  the NetBox tag — so a backfilled device can be in scope even if its included tag
  is not (yet) shown in NetBox, because feature tags only refresh on a clean
  collection. Fix Forward collection and the device collects clean next run and
  drops the tag automatically.
- **Out of scope** (orphan) — the device matches **none** of the included Forward
  tags. These are the removable ones: review them on the Scope Reconciliation page
  and delete with **Prune orphans**. (`device_tag_prune_out_of_scope` only deletes
  out-of-scope rows the sync query still returns; devices absent from the result
  entirely are removed by Prune orphans.)

So a device showing `forward-backfilled` but not an included tag in NetBox is
**not** an out-of-scope orphan — it is in scope and intentionally retained.

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

## Device CVE tab (netbox-dlm)

With the netbox-dlm plugin installed and the 2.5.2 **CVE / Vulnerability**
maps enabled, every device with findings gets a **CVEs** tab on its detail
page: severity totals plus one row per CVE (id, severity, affected software
version, description) — the actual CVEs behind the exposure count, no
Forward round-trip. The tab hides itself when a device has no findings and
is not registered at all on core installs without the plugin. Rows refresh
with each sync run that has the Vulnerability map enabled.

## Running a sync

- **Run** / **Adhoc Ingestion** enqueues the sync job.
- **Validate** runs query validation without applying.
- **Export Support Bundle** / **Export ZIP** collects diagnostics (live source
  health, query drift, data-file checks) for support.

## Push-triggered sync (webhooks)

Instead of polling on a schedule, an external system (e.g. a Forward webhook
that fires when a new snapshot finishes processing) can push a sync run.

**Preferred — the NetBox-native API path.** If the sender can set an
`Authorization` header, use the standard token-authenticated action; it gets
NetBox token auth, object permissions, and job provenance for free:

```bash
curl -X POST https://netbox.example.com/api/plugins/forward/sync/<id>/sync/ \
  -H "Authorization: Token <api-token>"
```

Create a dedicated service account with the `forward_netbox.run_forwardsync`
permission and a write-enabled token for this.

**Fallback — the shared-secret webhook endpoint.** For senders that cannot set
an `Authorization` header, set **Webhook secret** on the sync (Execution
section of the sync form; empty disables the endpoint), then:

```bash
curl -X POST https://netbox.example.com/api/plugins/forward/sync/<id>/webhook/ \
  -H "X-Forward-Webhook-Secret: <secret>"
# or, only if the sender cannot set ANY header (the secret will appear in
# access logs — prefer the header):
#   POST .../webhook/?secret=<secret>
```

Behavior:

- `202 {"status": "queued", "job_id": N}` — sync enqueued (attributed to the
  sync's configured user).
- `202 {"status": "already_running"}` — a run is already queued or running;
  nothing is re-queued, so webhook retries stay idempotent.
- `403` — wrong or missing secret, no secret configured, or unknown sync. The
  response is deliberately identical for all failure causes.
- `409` — the sync cannot start (e.g. waiting for a branch merge).

Use a long random secret and rotate it from the sync form. Combined with
**Skip scheduled runs on an unchanged snapshot**, a webhook per Forward
processing event keeps NetBox current without any polling schedule.

### Configuring Forward as the sender

Forward Enterprise ships an outbound webhook feature (**Settings > System >
Webhooks**, Forward Org Admin required) with a **"New Snapshot ready"**
(`SNAPSHOT_READY`) event — exactly the trigger for a push sync. Two Forward
limitations shape the setup:

- **Forward cannot set custom HTTP headers** on its webhooks (verified against
  the webhook schema: no header/auth/token fields; the only auth-adjacent
  option is a Basic Authentication toggle). The token path and the
  `X-Forward-Webhook-Secret` header path above are therefore not usable *from
  Forward* — they remain the right choice for senders that control headers
  (scripts, CI, middleware).
- The webhook URL is the one field Forward fully controls, so **use the query
  form** and treat the secret as rotatable:

```
https://netbox.example.com/api/plugins/forward/sync/<id>/webhook/?secret=<secret>
```

Setup in the Forward UI:

1. **Settings > System > Webhooks > Add a webhook.**
2. URL: the `?secret=` URL above. Content type: JSON.
3. Event: **New Snapshot ready**; select the network(s) the sync covers
   (empty = all networks).
4. The payload template is operator-editable (`$networkId`, `$snapshotId`,
   `$type` variables) — the NetBox endpoint ignores the body, so the default
   template is fine.
5. Use **Test connection** — it also shows the source IPs Forward will send
   from, useful when NetBox sits behind a firewall or allowlist.

Notes: the endpoint is idempotent for Forward's retries (an already-running
sync is acknowledged, not re-queued). Serve NetBox over HTTPS and remember a
query-string secret can appear in proxy/access logs — rotate it from the sync
form if exposed. Forward's delivery retry/timeout behavior is not documented;
keep a low-frequency scheduled sync with **Skip scheduled runs on an unchanged
snapshot** as the safety net for missed deliveries.

## Automating operator jobs (API + standing schedules)

The four operator buttons are also exposed as token-authenticated REST
actions, so an external scheduler can drive them:

```bash
POST /api/plugins/forward/sync/<id>/dependency-preview/
POST /api/plugins/forward/sync/<id>/prune-orphans/
POST /api/plugins/forward/sync/<id>/tag-delete-eligible-ipam/
POST /api/plugins/forward/sync/<id>/create-module-bays/
```

Each returns `201` with the job on success, `403` without permission, and
`202 {"status": "already_running", "job_id": N}` when an equivalent job is
already queued or running — duplicates never stack (across buttons, API
calls, and scheduled occurrences) and retries from a calendar-blind cron
stay green. Prune while a sync run is active answers
`202 {"status": "blocked_by_sync_run"}` instead: that prune did **not** run
(enable post-sync auto-prune to cover the gap, or retry after the sync). Permissions: in addition to each button's own permission (e.g.
`dcim.delete_device` for prune), NetBox's API token layer requires
`forward_netbox.add_forwardsync` for any POST to this viewset — grant both
to the service account. **Validate** (`POST .../validate/`) follows the same
contract. These four actions do not accept schedule parameters — a body
containing `schedule_at`/`interval` is rejected with `400` rather than
silently running once.

**Standing schedules.** Set them on the sync form (**Standing Schedules**
section: recurring validation / recurring dependency preview, in minutes;
blank disables) or via the API on `validate` and `dependency-preview`:

```bash
curl -X POST https://netbox.example.com/api/plugins/forward/sync/<id>/dependency-preview/ \
  -H "Authorization: Token <api-token>" -H "Content-Type: application/json" \
  -d '{"interval": 1440}'          # minutes; 0 cancels; optional future "schedule_at"
```

- An empty body keeps the one-shot behavior unchanged.
- `interval` creates a recurring schedule (one per sync per job type) and
  records the intent on the sync. Without `schedule_at` the first run starts
  immediately, then recurs. `schedule_at` must be in the future and requires
  `interval`. The preview enforces a 60-minute floor — it is a full live
  dry-run against Forward; on large fabrics schedule it **daily or less
  often**.
- Responses: `201` created/replaced, `200` when the identical schedule
  already existed (idempotent re-post), `200 {"status": "cancelled"}` for
  `interval: 0`.
- **To cancel**: blank the form field, or POST `{"interval": 0}`. Deleting
  the scheduled job from the Jobs list alone is not enough anymore — the
  recorded intent recreates it (see self-healing below). Deleting the sync
  cancels its schedules. Cancelling while an occurrence is mid-run is safe:
  the in-flight run finishes, then its chain reads the cancelled intent and
  stops itself.
- **Self-healing**: the schedule intent lives on the sync; the plugin
  re-creates a missing schedule at the end of every sync run, every
  occurrence re-checks the intent (a stale or duplicate chain re-aligns or
  stops itself), and schedules created on 2.5.6 (before intent storage) are
  adopted automatically. A worker hard-killed mid-occurrence (OOM/SIGKILL)
  no longer silently kills the schedule. The sync detail page shows each
  standing schedule and its next run.
- Editing the intent keys directly via a REST `PATCH` of `parameters` is
  validated (non-negative minutes; preview ≥ 60) but only takes effect at
  the next sync run, form save, or occurrence — the API actions above apply
  immediately.
- Recurrence requires the RQ scheduler. NetBox's `manage.py rqworker`
  command enables it unconditionally; only a worker started with a bare
  `rq worker` (bypassing the management command) lacks it.
- Recurring validation trims its own history: the newest 100 Validation Runs
  per sync are kept (configure with
  `PLUGINS_CONFIG["forward_netbox"]["validation_run_retention"]`; `0`
  disables trimming).

## Apply engine

Models apply through either the per-row **adapter** or the batched **bulk-ORM**
engine. The default safe set (all built-in models as of 1.5.10) runs bulk-ORM;
a handful of relationship-heavy models stay on the adapter. See
[Apply Engine Model Matrix](../02_Reference/apply-engine-model-matrix.md).

## Auditing Mgmt_ primary-IP resolution

When `set_primary_ip_from_mgmt_tag` is on, a device's `Mgmt_<iface>` tag sets its
primary IP from the IP on that interface. If few devices get a primary IP, this
read-only audit shows why, per device — it reuses the resolver's own matching, so
a verdict matches what a sync computes:

```
python manage.py forward_primary_ip_audit --sync-name "<sync>"
```

It reports `mgmt_tagged_devices`, `resolvable`, and the unresolved split:
`device_not_in_netbox`, `interface_not_matched` (the Mgmt target interface name is
not on the device in NetBox), and `interface_present_no_ip` (the interface exists
but no IP is assigned to it in NetBox — an import/assignment gap, since the
resolver reads NetBox assignments, not Forward). Never writes.

## Releasing the plugin

Maintainers cut releases with `invoke release` (see `scripts/release.py`):

```
invoke release --version X.Y.Z --summary "one-line note" --write
```

`--write` runs prepare (version bump + compatibility tables) and the local CI
mirror. Rollout (branch, push, tag, GitHub release, PyPI) only happens with
`--publish`/`--finish`, after GitHub CI is green. Pushing the `vX.Y.Z` tag
triggers the Trusted-Publishing workflow (`.github/workflows/release.yml`), which
builds and uploads to PyPI over OIDC with no stored token.

## Security and deployment hardening

The plugin has two operator-facing trust boundaries (see `SECURITY.md` for the
full policy):

- **Credential encryption + `SECRET_KEY`.** The Forward API password is encrypted
  at rest (Fernet, keyed off Django's `SECRET_KEY`) and masked/redacted in the
  UI/API/logs, so a database dump no longer contains a usable password. Protect
  `SECRET_KEY` accordingly, and note that **rotating `SECRET_KEY` requires
  re-entering the password on each Forward source** (old ciphertext can no longer
  be decrypted). Keep database backups access-controlled and give the Forward
  service account least privilege.
- **Restrict who can sync.** A sync performs inventory-wide create/update/delete
  across DCIM/IPAM and is not gated by NetBox object-level permissions. Treat
  creating a Forward source or triggering a sync as broad DCIM/IPAM write access
  and limit it to trusted operators. Destructive actions (device prune, IPAM
  delete-tagging) are dry-run-by-default and refuse to act on an empty Forward
  scope.

## Alerting

Two management commands surface problems without watching the UI; schedule them
(cron, systemd timer, or a NetBox custom script):

- `python manage.py forward_collection_gap_alert --fail-on-breach` — alerts when a
  sync's backfilled (tagged-but-not-freshly-collected) device count crosses a
  threshold.
- `python manage.py forward_stuck_job_alert --fail-on-stuck` — alerts when a
  forward_netbox background job is wedged (still PENDING/RUNNING in the database but
  with no live worker execution, e.g. a worker died or the heartbeat went stale).

Both print a JSON report and exit non-zero on breach when the `--fail-on-*` flag is
set, so a scheduler can treat a non-zero exit as the alert condition.

## Metrics

`python manage.py forward_metrics` emits plugin metrics in Prometheus
text-exposition format on stdout (source/sync/ingestion counts, jobs by status,
wedged-job count, and the age of the most recent completed job). Point a
node_exporter textfile collector at its output, or run it from a scrape sidecar, to
graph and alert on Forward sync health in Grafana/Datadog.

## Upgrades

See the [Upgrade and Rollback](upgrade.md) guide. Always back up the NetBox
database before upgrading; restore-from-backup is the supported rollback across a
schema migration, since some migrations are not fully reversible.
