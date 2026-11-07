# Operator Cleanup for Device Rows the Durable State Forgot

## Goal

An operator can see, and then remove, the interfaces, IP addresses, MAC
addresses, cables and BGP peers that Forward stopped returning on a run whose
in-scope device set changed before 3.0.14. Those runs took the
`contract_reset` path in `apply_durable_workload_deltas`, which staged no
deletes and replaced the stored state with the current target, so the rows are
in NetBox and in no state, and no later run will ever delete them.
`2026-11-06-scope-change-still-deletes.md` stops new ones; this clears the
backlog. Reachable from the UI (Scope Reconciliation), because customers have no
CLI (`2026-09-10-no-operator-cli-troubleshooting.md`).

## Constraints

- Preview first: the preview is read-only, makes no Forward call, and stores
  counts only. The removal recomputes and refuses anything beyond the preview
  the operator saw.
- A row is a candidate only if ALL of these hold:
  1. its model is in `STALE_ROW_CLEANUP_MODELS`, a subset of
     `DEVICE_SHARD_MODELS & BASELINE_REMOVAL_MODELS` whose reverse identity is
     exact;
  2. every device it belongs to is in the device list of the sync's latest
     finalized run (its current `dcim.device` state), is the only NetBox device
     with that name, is identified by no other sync, carries no
     out-of-scope/uncovered/backfilled claim, and has live (not carried) rows in
     this model's current state. Without a device list from the latest run,
     every model is refused (`no_device_list_from_the_latest_run`). Rows of devices that left scope are
     kept (`out_of_scope_entries`);
  3. no current durable state (this sync's or any other) holds its identity;
  4. the change log shows this sync's ingestions created it and only they
     changed it;
  5. the model's state exists, decodes, is from the latest finalized ingestion,
     describes most NetBox rows, and the absent share is within
     `MAX_REMOVAL_PERCENT` (30%, floor `MIN_REMOVAL_ROWS`).
- Never guess an identity. Matching is coarser than the stored identity
  (interface by exact name or canonical abbreviation, address by host, MAC by
  value, BGP peer by device + neighbour host), so a format difference can only
  make a row look present.
- Deletes go through the sync's own delete adapters (`runner._delete_<model>`,
  `delete_by_coalesce`, `protected_delete_skip`), one savepoint per row; a
  delete that resolves any other object is rolled back. Change-logged under the
  job's user. Refused while a sync run is active.
- Every UI figure, its split and when it was computed are in the support bundle.
- No customer names, hostnames, addresses or IDs anywhere.

## Touched Surfaces

- `forward_netbox/utilities/stale_row_cleanup.py` (new): candidate rules,
  per-model reverse identity, plan, preview, removal.
- `forward_netbox/jobs.py`: `_preview_stale_device_rows_work`,
  `_remove_stale_device_rows_work`, `PreviewStaleDeviceRowsJob`,
  `RemoveStaleDeviceRowsJob`.
- `forward_netbox/utilities/sync_facade.py`: two `BUTTON_JOB_SPECS` kinds; the
  removal joins the prune kinds blocked by an active sync run.
- `forward_netbox/api/views.py`: `preview-stale-device-rows`,
  `remove-stale-device-rows` actions (REST parity, enforced by
  `test_button_jobs`).
- `forward_netbox/views.py`: `_stale_row_cleanup_payload`, the card context,
  the bundle section `scope_reconciliation.stale_row_cleanup` (with
  `ui_figures` under the card's column labels), two action views.
- `forward_netbox/templates/forward_netbox/forwardsync_scope_reconciliation.html`:
  the "Stale Rows Forward No Longer Returns" card.
- `forward_netbox/utilities/bundle_label_index.py`: card -> bundle paths.
- `forward_netbox/tests/test_stale_row_cleanup.py` (new),
  `forward_netbox/tests/test_button_jobs.py` (work-function registry).
- `docs/01_User_Guide/operations.md`: "Stale rows Forward no longer returns".

## Approach

1. Scope: `in_scope_device_ids` = names in the sync's current `dcim.device`
   state from the latest finalized ingestion, mapped to NetBox devices where
   the name is unique, minus devices another sync identifies and devices with
   `out_of_scope`, `uncovered` or `backfilled` claims. The first version used
   identity records and scope claims; live it judged no device at all (scope
   claims exist only with "apply device scope tags" on, identity records only
   for some devices).
2. Per model: load the current state, refuse if missing / stale / unreadable;
   collect present keys from every upsert entry of every sync's current state;
   collect fetched device names from this sync's live upserts.
3. Collect NetBox rows on eligible devices (cables: both ends) with their keys
   and the delete row the adapter takes; refuse when fewer than half the judged
   state rows find a NetBox row (the mapping does not fit this estate).
4. Absent = no key present. Refuse past the brake. Hold: no reverse identity,
   template or module component, per-context address copy, no create record
   from this sync, any change outside this sync, locally referenced
   (`_locally_referenced_delete_identities`).
5. Cross-model: hold primary/OOB addresses; hold an interface that still
   carries an address, MAC, cable, child or LAG member that stays; hold a MAC
   that is primary on an interface that stays (fixed point).
6. Also refused per model: another baseline-ready sync with the model enabled
   has no comparable state (`_peer_delete_protection`, the sync's own gate);
   a live state row the cleanup cannot key. Held: management-step interfaces
   (`forward-management-interface`).
7. Before each delete, Django's deletion collector is asked what the delete
   would remove or blank; anything beyond the candidate, cable/tag
   bookkeeping, and sync-deletable rows this sync alone wrote holds the row.
   A BGP peer delete that removes a scope or router this sync did not create
   is rolled back. The removal re-checks no sync run is active.
8. Removal: needs a completed preview newer than the latest finalized run;
   refuses a model whose removable count grew; deletes in order bgppeer,
   cable, ipaddress, macaddress, interface; re-checks an interface is bare
   immediately before deleting it.

Models left out: `dcim.inventoryitem`, `dcim.module` (identity variants the
module/inventory paths rewrite), `ipam.fhrpgroup` (shared by several devices),
`extras.taggeditem` (owned by the scope-tag job), `netbox_routing.bgp*family`,
`netbox_routing.ospf*` (cascade with their parent, or several generic-key hops
to reverse), `netbox_peering_manager.peeringsession` (goes with its peer).

## Validation

- `invoke test-isolated --test-label forward_netbox.tests.test_stale_row_cleanup`
- `invoke test-isolated --test-label forward_netbox.tests.test_button_jobs`
- `invoke test-isolated --test-label forward_netbox.tests.test_bundle_label_index`
- `invoke test-isolated --test-label forward_netbox.tests.test_log_export`
- `invoke harness-check`, `pre-commit run --files <changed files>`
- Live, before release: on the customer-shaped stack, preview then remove, and
  confirm the interface and BGP peer counts fall by the previewed amounts with
  nothing hand-added touched.

Evidence: `test_stale_row_cleanup` passes on the test host, and the full suite
passed (4452 tests) before the device-list change. Live on the customer-shaped
stack through the UI (agent-browser): the preview found 174 removable (158 BGP
peers, 11 interfaces, 5 cables) and held 198 (178 management-step interfaces,
17 primary or out-of-band addresses, 3 per-context copies); removal deleted
exactly those 174 in about 70 seconds, and the next sync merged with no
failures.

## Rollback

Revert the commit. The feature adds no migration and stores only job data; a
removal already run is in NetBoxs change log (each delete under the job's
user) and the rows return on the next sync only if Forward reports them again.

## Decision Log

- Operator-triggered, not automatic: these rows predate any state that could
  prove they were Forward's; an unattended delete on that evidence is exactly
  what the prune quarantine exists to prevent.
- Change-log ownership rather than ingestion records: no per-row record of what
  an ingestion wrote survives promotion. Consequence: rows older than NetBox's
  changelog retention are held (`no_create_record_from_this_sync`), never
  guessed. Device-type template interfaces share their device's create
  request, so they are held by name match against the template.
- Coarser-than-identity matching instead of re-deriving `canonical_row_identity`
  from NetBox: an exact re-derivation fails open on any formatting difference
  (mask length, MAC separators, abbreviations); a coarse match fails closed.
- Other syncs' states count as "present": a row any sync still asserts is not
  stale, whoever created it.
- Recomputed in the job and capped by the preview, rather than replaying stored
  pk lists: the job never acts on rows the operator did not see counted, and
  never on a stale list.
