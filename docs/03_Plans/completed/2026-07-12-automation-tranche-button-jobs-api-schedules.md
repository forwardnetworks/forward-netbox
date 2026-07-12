# 2.5.6 Automation Tranche: Button-Job Guardrails, REST Parity, Standing Schedules, Prune Danglers

(Developed as the "2.6" tranche; released as 2.5.6.)

## Goal

Make the operator "button jobs" (dependency preview, prune orphans, tag
delete-eligible IPAM, create module bays) safe to automate: a shared overlap
guard so duplicates cannot stack, REST API parity so external schedulers can
drive them, native NetBox standing schedules (JobRunner + recurrence) for
dependency preview and validation, and a scoped post-prune sweep for dangling
`netbox_routing` GenericFK rows that would otherwise accumulate silently.

## Constraints

- No release from this branch until explicitly cut; feature branch only.
- Job-name couplings are load-bearing and must stay byte-identical for
  immediate runs: the drift report and preview GET match
  `icontains "dependency preview"`, the webhook checks
  `"<sync> - adhoc"`/`"- scheduled"`, auto-prune appends `" (auto)"`.
- The post-sync auto-prune enqueues from inside the still-running sync job;
  the overlap guard must not deadlock it (`during_sync_ok` escape).
- Recurrence must use core `JobRunner.handle()` semantics — a bare
  `Job.enqueue(interval=...)` on a plain function is inert (never re-enqueues).
- `netbox_routing` stays optional: the dangler sweep must no-op cleanly when
  the plugin is absent.
- Standing schedules require the RQ scheduler (`rqworker --with-scheduler`).

## Touched Surfaces

- `forward_netbox/utilities/sync_facade.py` — `JobAlreadyActive`,
  `BUTTON_JOB_SPECS`, `enqueue_button_job`, schedule params on
  `enqueue_validation_job`, `enqueue_preview_schedule`.
- `forward_netbox/jobs.py` — shared work bodies + legacy shims,
  `DependencyPreviewJob`/`ValidationJob` (JobRunner), auto-prune rewired
  through the shared guard, validation object-rebind removed.
- `forward_netbox/views.py` — four button POST handlers use the shared guard.
- `forward_netbox/api/views.py` — four new `@action` endpoints,
  `_parse_schedule`, schedule params on `validate` + `dependency-preview`.
- `forward_netbox/api/serializers.py` — `JobScheduleRequestSerializer`.
- `forward_netbox/models.py` — `enqueue_validation_job` wrapper passthrough.
- `forward_netbox/utilities/scope_reconciliation.py` +
  `utilities/branch_budget.py` — `_cleanup_dangling_routing_objects` sweep +
  delete-order rank entries.
- Tests: `tests/test_button_jobs.py` (new),
  `tests/test_scheduled_jobs.py` (new),
  `tests/test_device_scope_reconciliation_audit_command.py` (extended).

## Approach

Four chunks, one commit each:

1. **Shared overlap guard** (`a4fe444`): one spec table + one enqueue helper;
   prefix-match dedup (`name__startswith`) so `"prune orphans (auto)"` and
   manual prune block each other; prune additionally blocked while a sync is
   active unless `during_sync_ok`; HTML views raise → `messages.warning`.
2. **REST parity** (`3812b76`): four detail actions on `ForwardSyncViewSet`
   sharing one body — permission from the spec, 201 + JobSerializer, 409 on
   `JobAlreadyActive` (mirrors the sync action's SyncError→409 pattern).
3. **JobRunner port + schedules** (`95e64f4`): extract shared work bodies;
   keep the plain functions as dotted-path shims for pre-existing queued Job
   rows and immediate runs (legacy per-sync names); add JobRunner classes with
   fixed `Meta.name` so `enqueue_once` dedup (cls.name + instance) yields one
   standing schedule per sync; drop the validation job's object rebind (under
   recurrence it would re-enqueue against the validation run instead of the
   sync) and expose the run via `job.data["validation_run_id"]`; API accepts
   optional `schedule_at`/`interval` (minutes, ≥1).
4. **Routing dangler sweep** (`81293b3`): after device prune, delete
   `netbox_routing` rows whose GenericFKs pointed at pruned devices
   (routers → scopes → AFs/peers → settings), one atomic transaction,
   dependency-ranked order, tally reported as `pruned_dangling_rows`.

## Validation

- Full suite: 1077 tests OK (28 skipped) at chunk-3 HEAD; pre-commit green.
- 22 targeted tests across the two new modules pin the name literals, guard
  semantics, API status codes (201/403/409/400), schedule routing to
  `enqueue_once`, the sync-binding invariant, and empty-body fall-through to
  legacy behavior.
- Live smoke on the dev stack: enqueue_once idempotency proven (same params
  -> same Job pk, interval change -> single replaced row, never >1 standing
  row) — this caught the schedule_at-defaulting churn fixed in b52fa2f.
- Post-tranche 44-agent audit (6 lenses, adversarial verify): 37 confirmed
  findings; the blocker (standing SCHEDULED row permanently suppressing
  snapshot catch-up) and all should-fixes rolled into the 2.5.6 hardening
  commit; see the release-2.5.6 plan's Decision Log for accepted deferrals.

## Rollback

Revert the four commits (independent; reverse order safest). The validation
rebind removal is data-shape only: old Job rows bound to validation runs
remain readable; new rows carry `validation_run_id` in `data`. No migrations
in this tranche.

## Decision Log

- Immediate runs keep legacy per-sync names; only standing schedules use the
  fixed JobRunner names — satisfies every existing name coupling without a
  rename migration.
- Sync's bespoke self-reschedule loop is intentionally untouched: it predates
  JobRunner recurrence and encodes retry/backoff behavior the port would lose.
- Recurring preview on large fabrics is a full dry-run against live Forward
  data; docs recommend ≥ daily cadence.
- Cancellation UX = delete the scheduled Job row from the Jobs list (core
  NetBox behavior); no bespoke cancel endpoint.
- Deferred (explicit non-goals): sync-loop JobRunner convergence, JobRunner
  conversion of prune/tag/module-bay jobs, a `forward_routing_dangling_audit`
  command, query consolidation.
