# Operator cleanup for routing policy no in-scope device holds

## Goal

The routing policy maps are global catalogues. A prefix list, community list
or route map that only devices outside the sync's include tags hold is dropped
by the device-tag scope, never staged again, and, if NetBox already holds it,
kept indefinitely. The previous change makes the drift report count those
entries as "outside scope" instead of pending removal. Give the operator a way
to remove them. The action is operator-initiated, gated like the prunes, and
allowlisted to the six catalogue models.

## Constraints

- **Nothing runs on its own.** The sync never deletes these rows; only the
  button does.
- **The candidate set is recomputed from a fresh Forward read** inside the
  job, never taken from the preview's stored count.
- **Only entries this sync provably created are deleted.** Proof is NetBox's
  change log: a `create` ObjectChange whose `request_id` is one of this
  sync's ingestions' `change_request_id`. An entry with no such record (an
  operator's, another sync's, or one whose change log has aged out) is held.
- **Nothing a sync still asserts is deleted.** A definition name any sync's
  current durable workload state asserts as an upsert is held.
- **No route map is silently broken.** A prefix or community list that a
  route map outside the cleanup matches on is held. These links are
  many-to-many, not PROTECT, so a delete would drop them without error.
- **Refuse rather than guess:**
  - no device-tag scope configured at all;
  - an empty in-scope result for a model;
  - a model disabled on the sync;
  - candidates above half of the model's entries, above a floor of 25.
- **Allowlist.** `CATALOGUE_CLEANUP_MODELS` is the six netbox_routing
  catalogue models. The delete path refuses anything else before touching it.
- **Same concurrency rules as the other prunes.** The job is refused while a
  sync run is active, and deletes run under `ownership_write_lock`, one
  transaction per list.

## Touched Surfaces

- `forward_netbox/utilities/routing_catalogue_cleanup.py`:
  - `plan_out_of_scope_catalogue_cleanup` and `prune_out_of_scope_catalogue`;
  - the hold and refusal reasons, and the fraction guard.
- `forward_netbox/jobs.py`: `_prune_out_of_scope_catalogue_work` and
  `PruneOutOfScopeCatalogueJob`.
- `forward_netbox/utilities/sync_facade.py`: the
  `BUTTON_JOB_SPECS["prune_out_of_scope_catalogue"]` spec, blocked while a
  sync runs.
- `forward_netbox/views.py`: `ForwardSyncPruneOutOfScopeCatalogueView` and
  `_catalogue_cleanup_payload`.
- `forward_netbox/api/views.py`: the `prune_out_of_scope_catalogue` action.
- Templates:
  - `forwardsync_scope_reconciliation.html`: the "Routing Policy Outside
    Scope" card, with the count from the latest preview;
  - `forwardsync_drift_report.html`: a link from the outside-scope line.
- Tests: `forward_netbox/tests/test_routing_catalogue_cleanup.py` and the
  `test_button_jobs.py` parity entry.

## Approach

1. **Fresh read.** `ForwardQueryFetcher.fetch_workloads` runs the three
   catalogue models, and its `catalogue_scope_names` gives the definitions
   the scope kept and dropped.
2. **Plan, per enabled model.**
   - Candidates are the entries whose parent is a dropped definition that is
     not also kept (case-insensitive).
   - The fraction guard is checked next.
   - Then each candidate is held for having no create record from this sync,
     or for being asserted by any sync's current state.
   - Lists matched by a staying route map are then held.
3. **Delete.** Per list, in one locked transaction: delete the planned
   entries, then the list if it is now empty. A `ProtectedError` holds that
   list.
4. **Report.** For each model, the job records the candidate and entry
   counts, deleted entries and lists, and hold counts by reason. It never
   records a list name.

## Validation

- `test_routing_catalogue_cleanup.py`:
  - out-of-scope entries this sync created are removed, and the emptied list
    with them;
  - in-scope entries are untouched;
  - a list stored under another spelling is matched;
  - entries with no create record, or created by another sync's ingestion,
    are held;
  - a definition a sync asserts is held;
  - a list a staying route map matches on is held;
  - an empty in-scope result, a disabled model and too large a share are all
    refused, with nothing deleted;
  - no device is ever touched;
  - the allowlist is exactly the six models;
  - no device-tag scope refuses before any Forward fetch.
- `test_button_jobs.py`: the spec, runner name and work function are in
  parity.
- Regression: the Scope Reconciliation view tests, then the full `invoke ci`.

## Rollback

No migration and no model change. Reverting removes the button; the drift
report keeps counting the entries.

## Decision Log

- **Change-log provenance over durable state as the authorship proof.** The
  durable state records what a sync currently asserts, which is not the same
  as having created the row; the upsert adopts any existing row with the same
  name. A `create` record stamped with this sync's request id is the only
  proof that the plugin made it.
- **An aged-out change log holds rather than deletes.** NetBox prunes change
  records after its retention period, and the cleanup cannot tell an
  aged-out record from an operator-made row. Holding is the safe answer, and
  the hold reason says so.
- **The count on the page comes from the preview; the action does its own
  read.** A page load must not call Forward, and a delete must not act on a
  stored number.
