# Routing policy catalogue: honest scope and honest pending removals

## Goal

A customer's dependency preview reported about 6,300 pending removals across
the prefix-list, community-list and route-map maps on every run, so the
preview could never read "in sync". A live measurement against their network
explains every one of them:

- Their source prunes out-of-scope rows, so every catalogue definition only
  devices outside the include tags hold is declared a removal on every run.
- The durable workload state tombstoned those rows long ago and stages none
  of them. Its `tombstone_count` equals the declared removals and its
  `staged_delete_count` is 0. Nothing is left to remove.
- Separately, each catalogue row names only a representative device (the
  lowest holder across the whole network). The tag scope tested that one
  device, so a definition in-scope devices use was dropped whenever its
  lowest holder was out of scope. Live, that recovers 13 prefix-list,
  19 community-list and 5 route-map entries.
- The catalogue delete path resolved a list's parent by exact name, while the
  write path matches case-insensitively. An entry under a differently-cased
  parent was never deleted.

Report pending removals as what the next sync removes, scope a definition by
every device that holds it, and delete what the write path would find.

## Constraints

- **No `@query` parameter change.** The three queries only gain an output
  column (`scope_holders`), so a stale published org copy keeps working and
  behaves as before until republished. The signature preflight confirms no
  parameter changed.
- **Never report fewer pending removals than the next sync stages**, and never
  more than Forward declared.
- **No new automatic deletion.** Deleting catalogue entries no in-scope device
  holds is a separate, operator-initiated action (the next PR).

## Touched Surfaces

- Queries: `forward_routing_prefix_lists.nqe`,
  `forward_routing_community_lists.nqe`, `forward_routing_route_maps.nqe`
  (`scope_holders` on each definition's lowest-sequence row).
- `utilities/query_fetch_execution.py`:
  - `ROUTING_CATALOGUE_MODELS` and `_catalogue_definitions_in_scope`;
  - the catalogue branch of `_apply_device_tag_scope`;
  - `ForwardQueryFetcher.catalogue_scope_names`.
- `utilities/sync_routing_policy.py`: `_resolve_policy_parent` falls back to a
  case-insensitive match.
- `utilities/routing_catalogue_cleanup.py` (new):
  `outside_scope_entry_queryset` and the six-model allowlist.
- `views.py`:
  - `_dependency_model_result_summary`: `delete_count` from the durable
    state's staged count, plus `forward_removal_count` and
    `already_removed_count`;
  - `_catalogue_outside_scope_in_netbox`.
- `utilities/drift_report.py` and
  `templates/forward_netbox/forwardsync_drift_report.html`: both counts per
  model and in the summary, kept out of drift.
- Tests:
  - new: `test_routing_catalogue_scope.py`, `test_routing_catalogue_drift.py`;
  - extended: `test_sync_routing_policy.py`.

## Approach

1. **Query side.**
   - `defs` and `variants` carry `first_seq`; `ranked` carries `all_holders`.
   - The final select adds
     `scope_holders: (foreach h in r.all_holders where e.seq == r.first_seq select h)`.
   - An empty-list literal is rejected at NQE runtime ("Can't handle empty
     list literals now!"), found live, so a filtered `foreach` is used
     instead.
2. **Scope.** For a catalogue model, the scope groups rows by stored `name`
   and keeps the whole definition when any of `device`, `holder_devices` or
   `scope_holders` is in scope. Kept and dropped names are recorded on the
   fetcher per model.
3. **Pending removals.**
   - When the model's durable-state diagnostic carries a `staged_delete_count`
     no larger than Forward's declared removals, that is the pending count.
   - The difference is `already_removed_count`.
   - Sibling maps sharing the state keep their own declared count.
4. **Outside scope in NetBox.** For each catalogue model, the preview counts
   the NetBox entries whose parent is a definition the scope dropped and no
   in-scope device holds, matched case-insensitively. The drift report shows
   this as "outside scope", never as drift.
5. **Delete path.** `_resolve_policy_parent` retries a miss with
   `name__iexact` and takes a unique hit.

## Validation

- **Live**, against the customer's network, on its current snapshot:
  - old and new queries return identical row counts (11,918 / 2,850 / 5,486);
  - every definition carries `scope_holders`, except 4 community lists, which
    fall back to today's rule;
  - the representative is always among the holders;
  - `scope_holders` adds 4,994 / 5,243 / 5,986 device names in total;
  - on the customer's 3,554 in-scope devices, the old rule keeps exactly the
    8,759 / 2,155 / 3,042 rows their sync log reported, and the new rule
    keeps 8,772 / 2,174 / 3,047.
- **`test_routing_catalogue_scope.py`:**
  - a shared definition with an out-of-scope representative is kept;
  - a definition only out-of-scope devices hold is dropped whole;
  - a stale published query keeps the old behaviour;
  - holders are unioned across a definition's rows;
  - other models are unchanged.
- **`test_routing_catalogue_drift.py`:**
  - tombstoned removals are not pending, and newly staged ones are;
  - no durable state and shared siblings keep the declared count;
  - a staged count above the declared one is not trusted;
  - already removed and outside scope are never drift;
  - older payloads read as zero.
- **`test_sync_routing_policy.py`:** a delete finds a parent stored under
  another spelling; outside-scope entries match by parent name in any case.
- Regression: the drift, preview, durable-state attribution and NQE lint
  test modules, then the full `invoke ci`.

## Rollback

No migration. Reverting the queries is safe for published copies, because the
column is only additive. Reverting the summary change restores the inflated
pending count.

## Decision Log

- **Staged count over a NetBox-side existence check for pending removals.**
  The durable state already decides what the next sync removes, per identity.
  Re-deriving that from NetBox would be a second, slower answer to the same
  question, and could disagree with it.
- **Holders once per definition, not on every row.** A widely shared list
  would repeat thousands of device names on each of its entries. One row per
  definition carries the full list, 16k names in total on this estate.
- **Republish is optional, not required.** Without it a pinned org copy keeps
  scoping by representative, as today. The changelog says so.
