# Operator Cleanup for Device Rows the Durable State Forgot

## Goal

An operator can see, and then remove, the interfaces, IP addresses, MAC
addresses, cables, inventory items, modules, BGP peers, BGP address families,
BGP peer address families and OSPF instances and interfaces that Forward stopped
returning on a run whose
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
     changed it - or, only when the operator opted in (see "Rows the change log
     no longer remembers"), the log provably cannot speak for it;
  5. the model's state exists, decodes, is from the latest finalized ingestion,
     describes most NetBox rows, and the absent share is within
     `MAX_REMOVAL_PERCENT` (30%, floor `MIN_REMOVAL_ROWS`).
- Never guess an identity. Matching is coarser than the stored identity
  (interface by exact name or canonical abbreviation, address by host, MAC by
  value, BGP peer by device + neighbour host, inventory item by device + name,
  module by device + bay name, BGP address family by device + VRF + family under
  every spelling the sync accepts, peer address family by device + neighbour +
  family, OSPF instance by device + VRF + process id as the sync stores it, OSPF
  interface by device + interface under the routing abbreviations), so a format
  difference can only make a row look present.
- A parent row (BGP peer, BGP address family, OSPF instance) is also present
  while a child model's current state (any sync's) still names it, because
  applying the child writes the parent; a child model enabled on the sync with
  no current state refuses the parent model. A BGP address family and an OSPF
  instance go only when every child on them is itself leaving (peer address
  families, OSPF interfaces); an interface goes only when its addresses, MACs,
  cables, children, LAG members, OSPF interfaces and inventory items are
  leaving.
- Deletes go through the sync's own delete adapters (`runner._delete_<model>`,
  `delete_by_coalesce`, `protected_delete_skip`), one savepoint per row; a
  delete that resolves any other object is rolled back. Change-logged under the
  job's user. Refused while a sync run is active.
- Every UI figure, its split and when it was computed are in the support bundle.
- No customer names, hostnames, addresses or IDs anywhere.

## Touched Surfaces

- `forward_netbox/utilities/stale_row_cleanup.py` (new): candidate rules,
  per-model reverse identity, plan, preview, removal, the change-log opt-in.
- `forward_netbox/jobs.py`: `_preview_stale_device_rows_work`,
  `_remove_stale_device_rows_work`, `PreviewStaleDeviceRowsJob`,
  `RemoveStaleDeviceRowsJob` (both take `include_older_than_change_log`).
- `forward_netbox/utilities/sync_facade.py`: two `BUTTON_JOB_SPECS` kinds; the
  removal joins the prune kinds blocked by an active sync run.
- `forward_netbox/api/views.py`: `preview-stale-device-rows`,
  `remove-stale-device-rows` actions (REST parity, enforced by
  `test_button_jobs`); both accept a JSON boolean `include_older_than_change_log`.
- `forward_netbox/views.py`: `_stale_row_cleanup_payload`, the card context,
  the bundle section `scope_reconciliation.stale_row_cleanup` (with
  `ui_figures` under the card's column labels: the option, the count it adds,
  the oldest retained change and when the preview computed them), two action
  views (the preview reads the checkbox).
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

### Rows the change log no longer remembers (opt-in)

NetBox keeps 90 days of change log by default, so on an estate with months of
stale rows most are held as `no_create_record_from_this_sync`. The operator can
opt in with "Also remove rows older than the change log" (preview checkbox; REST
and job parameter `include_older_than_change_log`).

- It reaches only a row with no create record whose own `created` time is OLDER
  than the oldest `ObjectChange` NetBox retains anywhere, by anyone: the log
  provably cannot speak for it. A row created at or after that point stays held
  (`no_create_record_from_this_sync`). An empty log reaches nothing.
- A row with ANY retained change by something other than this sync's
  ingestions - an update, or someone else's create record - stays held always.
  Every other rule (device in the latest device list, absent from every state,
  the brake, the cascade check, hand-edit holds, template, management and
  primary-address holds, the ownership check on a BGP peer's scope and router)
  applies unchanged. Consequence: an old peer whose scope or router would also
  go, and which have no create record either, is rolled back
  (`bgp_scope_or_router_not_created_by_this_sync`).
- The preview stores the choice (`options.older_than_change_log`) and the oldest
  retained change it measured against (`oldest_change_logged_at`). The removal
  takes the preview's choice and pins that same change-log point, so pruning the
  log in between cannot widen it. `include_older_than_change_log=True` against
  a preview that did not opt in is refused
  (`option_wider_than_the_reviewed_preview`); `False` narrows. A model whose
  option-only count grew since the preview is refused, like any growth.
- With the option off, rows it would reach are counted as the hold
  `no_create_record_older_than_the_change_log` (an upper bound, counted before
  later holds), so the operator can see what ticking it would add. With it on,
  the preview shows a separate "Older than change log (option)" count per model
  and in total. The removal confirm says the operator attests nobody created or edited
  those rows by hand.
- Support bundle: the option, the per-model and total counts it adds, the
  oldest retained change time and the preview's computed-at are under the
  card's labels in `ui_figures` and in the section's own keys; the last
  removal's `deleted_older_than_change_log` is under "Last removal ... deleted
  only because of the option".

### Models

Covered (delete order, children first): `netbox_routing.bgppeeraddressfamily`,
`bgpaddressfamily`, `bgppeer`, `ospfinterface`, `ospfinstance`, `dcim.cable`,
`ipam.ipaddress`, `dcim.macaddress`, `dcim.inventoryitem`, `dcim.module`,
`dcim.interface`. For each, the identity came from the model's coalesce fields
(`sync_contracts.MODEL_SYNC_CONTRACTS`), the NQE row, and the sync's own delete
adapter:

- `dcim.inventoryitem`: coalesce `(device, name, part_id, serial)` falling back
  to `(device, name)`; NetBox side is the item's own fields. Matched coarsely by
  device + name. With `dcim.module` enabled, module-native state rows are not
  expected in NetBox (the sync deletes them) and are left out of the
  state-matches-NetBox check. Items instantiated from a device type's inventory
  item template are held. Delete: `_delete_dcim_inventoryitem`.
- `dcim.module`: coalesce `(device, module_bay)`; NetBox side is the module's
  bay; the apply stores the cleaned bay name, so both spellings match. A module
  with any component installed (every reverse `module` foreign key, read from
  the schema) is held: deleting it deletes the component, which Forward may
  still return. Delete: `_delete_dcim_module`.
- `netbox_routing.bgpaddressfamily`: coalesce `(device, vrf, local_asn,
  afi_safi)`; NetBox side is scope -> router -> device, scope -> VRF and the
  stored family. The ASN is left out of the key (coarser). A family a peer
  address family state still names is present; one with a peer address family
  that stays is held (the foreign key is PROTECT). Delete:
  `_delete_netbox_routing_bgpaddressfamily`.
- `netbox_routing.bgppeeraddressfamily`: coalesce `(device, vrf,
  neighbor_address, afi_safi)`; NetBox side is the BGP peer it hangs on (an
  address family hung on anything else has no device and is not seen), its
  neighbour host and the family. Delete:
  `_delete_netbox_routing_bgppeeraddressfamily`.
- `netbox_routing.ospfinstance`: coalesce `(device, vrf, process_id)`; the
  state's process id is converted exactly as the apply converts it (a
  non-numeric id is stored as a hash of device, VRF, id and domain). An instance
  an OSPF interface state still names is present; one with an OSPF interface
  that stays is held (PROTECT). Delete: `_delete_netbox_routing_ospfinstance`.
- `netbox_routing.ospfinterface`: coalesce `(device, process_id,
  local_interface)`; NetBox's identity is the interface alone, so the key is
  device + interface under the abbreviations the routing lookup accepts.
  Delete: `_delete_netbox_routing_ospfinterface`.

Cross-model: the interface cleanup now also holds an interface that still
carries an OSPF interface or an inventory item attached to it. A BGP peer is
also present while a peer address family or peering session state names it, and
the peer model is refused when an enabled child model has no current state.

Left out, with the specific reason:

- `ipam.fhrpgroup`: the state's identity is a participant (protocol, group,
  virtual address, VRF, device, interface), but the NetBox object that carries it
  is split three ways: the assignment (device-scoped), the group (shared by
  several devices) and the virtual IP (a separate address that two groups may
  share, attached to only one of them, so a group can have none). The delete
  adapter finds the group by a name built from the address and VRF and, when the
  last participant leaves, deletes the group and the virtual IP. A group without
  the shared IP cannot be mapped back to the row's address, and those side
  effects land on objects no single device owns.
- `netbox_routing.ospfarea`: a global catalogue row, not device-scoped (not a
  device-shard model); shared by every device that uses the area.
- `netbox_peering_manager.peeringsession`: its identity is exact (it follows its
  BGP peer), but it is not in `BASELINE_REMOVAL_MODELS`: the sync's own
  durable-state path never removes a session by absence, only a Forward diff or
  its peer's cascade does, and rule 1 does not widen past what the sync itself
  may delete. A session on a stale peer holds that peer (cascade check).
- `extras.taggeditem`: a tag assignment, owned by the scope-tag job and its
  claims, and not in `BASELINE_REMOVAL_MODELS`.

A test fails if a device-shard model is neither covered nor named in
`STALE_ROW_CLEANUP_LEFT_OUT`.

## Validation

- `invoke test-isolated --test-label forward_netbox.tests.test_stale_row_cleanup`
- `invoke test-isolated --test-label forward_netbox.tests.test_button_jobs`
- `invoke test-isolated --test-label forward_netbox.tests.test_bundle_label_index`
- `invoke test-isolated --test-label forward_netbox.tests.test_log_export`
- `invoke harness-check`, `pre-commit run --files <changed files>`
- Live, before release: on the customer-shaped stack, preview then remove, and
  confirm the interface and BGP peer counts fall by the previewed amounts with
  nothing hand-added touched.

Not yet run (written without access to the shared test stack): the change-log
option and the six added models in `test_stale_row_cleanup`. Run them, then
exercise the option and each model live before release, as the standing rule
requires.

Evidence for the first version: `test_stale_row_cleanup` passes on the test host, and the full suite
passed (4452 tests) before the device-list change. Live on the customer-shaped
stack through the UI (agent-browser): the preview found 174 removable (158 BGP
peers, 11 interfaces, 5 cables) and held 198 (178 management-step interfaces,
17 primary or out-of-band addresses, 3 per-context copies); removal deleted
exactly those 174 in about 70 seconds, and the next sync merged with no
failures.

## Rollback

Revert the commit. The feature adds no migration and stores only job data; a
removal already run is in NetBox's change log (each delete under the job's
user) and the rows return on the next sync only if Forward reports them again.

## Decision Log

- Operator-triggered, not automatic: these rows predate any state that could
  prove they were Forward's; an unattended delete on that evidence is exactly
  what the prune quarantine exists to prevent.
- Change-log ownership rather than ingestion records: no per-row record of what
  an ingestion wrote survives promotion. Consequence: rows older than NetBox's
  changelog retention are held (`no_create_record_from_this_sync`), never
  guessed, unless the operator opts in and attests (above). The opt-in is
  bounded by the row's own `created` time against the oldest retained change,
  not by a guess, and by the change-log point the reviewed preview recorded.
  Device-type template interfaces share their device's create request, so they
  are held by name match against the template.
- Parent presence from child states: applying a peer address family writes its
  peer and address family, and an OSPF interface its instance, so a parent
  absent from its own state can still be required every run. Deleting it would
  only churn (and PROTECT would refuse it), so a child state naming it keeps it.
- Coarser-than-identity matching instead of re-deriving `canonical_row_identity`
  from NetBox: an exact re-derivation fails open on any formatting difference
  (mask length, MAC separators, abbreviations); a coarse match fails closed.
- Other syncs' states count as "present": a row any sync still asserts is not
  stale, whoever created it.
- Recomputed in the job and capped by the preview, rather than replaying stored
  pk lists: the job never acts on rows the operator did not see counted, and
  never on a stale list.
