# No operator CLI troubleshooting: diagnostics and remediation in the GUI

## Goal

An operator never has to run a management command to understand or fix
something. Every count on the scope-reconciliation panel is explicable and
actionable from the page it appears on, the full set behind every count is
listable, the remediation for each is a gated button, and a string an operator
reads never names a command to paste.

Phase 1 - the surfaces that fire during an incident - ships in `2.9.4` and
`3.0.1`. A build-failing guard ships with it so the rule cannot silently
regress.

## Constraints

- 2.9.x lane: NetBox 4.6.x, `netbox_branching` 1.1.3, no optional plugin
  installable on 4.7. Written here first, then forward-ported to `main`.
- No new model, so no migration and no `urls.py` change: `urls.py` includes
  `get_model_urls` wholesale and `@register_model_view` is enough for a view
  hung off an existing model.
- Persisted diagnostics carry schema identifiers and primary keys, never
  collected values or device names. The two new full-list views read primary
  keys added to the report payload for exactly this reason.
- Destructive safety is unchanged. The absence quarantine, the survivable-shrink
  refusal, the empty-scope refusal, the cause gating and the active-sync block
  all stay as they are; this moves where the control lives, not what it may do.
- `ForwardDeviceIdentity.device` and `ForwardDeviceTagClaim.device` stay
  `PROTECT`. See the Decision Log.

## Touched Surfaces

- `scripts/check_operator_text.py` (new), wired into `invoke ci` via
  `tasks.py::operator_text_check`.
- `forward_netbox/jobs.py` - `PruneUncoveredDevicesJob`, `RecoverStuckSyncJob`
  and their work functions.
- `forward_netbox/views.py` - the two POST views, `stuck_verdict` on the sync
  detail context, and `ForwardSyncOutOfScopeDevicesView` /
  `ForwardSyncPresentBackfilledDevicesView` as subclasses of the existing
  `_ForwardSyncUncoveredDevicesView`.
- `forward_netbox/utilities/sync_facade.py` - two `BUTTON_JOB_SPECS` entries and
  the active-sync block extended to `prune_uncovered`.
- `forward_netbox/api/views.py` - REST `@action`s for the three button kinds
  that had none.
- `forward_netbox/utilities/scope_reconciliation.py` - `out_of_scope_device_ids`
  and `present_backfilled_device_ids` in the persisted payload.
- `forward_netbox/utilities/ingestion_issues.py` - `blocking_issue_q` and
  `is_blocking_issue` extracted from `blocking_issues_queryset`.
- `forward_netbox/tables.py`, `filtersets.py`, `forms.py` - the Blocking column,
  the Blocking filter and the filter form that makes it clickable.
- `forward_netbox/navigation.py` - an `Ingestion Issues` menu item for a route
  that existed and nothing reached.
- `forward_netbox/template_content.py` plus
  `templates/forward_netbox/inc/device_ownership_panel.html` - the Forward
  Ownership panel on the device page.
- `templates/forward_netbox/forwardsync_scope_reconciliation.html` - the
  Prune-uncovered form, permission gates on the footer, and List-all links for
  the two newly listable sets.

## Approach

**The guard first.** `scripts/check_operator_text.py` scans Python string
literals (excluding docstrings, `management/`, `tests/`, `migrations/`) and
every template for `manage.py`, `invoke `, `docker compose`, `rqworker` and this
plugin's own command names, which are read from the commands directory rather
than matched by pattern - a pattern over `forward_*` matched a database
constraint name as readily as a command. The allowlist is three entries
(`manage.py migrate`, `pip install`, `manage.py check`), each carrying a comment
saying why it cannot be a GUI action. It failed on three violations before those
were reworded; it exits 0 now.

The second guard is `test_button_jobs.py` iterating `BUTTON_JOB_SPECS` instead
of a hardcoded three, so a new destructive kind cannot be added with no REST
parity, no permission and no runner-name check.

**Then the four surfaces that block an operator mid-incident:**

1. *Prune uncovered devices.* The job, the runner, the POST view and the REST
   action, plus a gated form in the "Uncovered, Created by This Sync" card. The
   count comes from `unmanaged.owned_prune_candidates` - what the gates would
   actually delete - not `owned_untagged`, so the button never promises a
   deletion the quarantine refuses. `include_quarantined` renders only when
   something is held.
2. *Recovering a wedged sync.* `recover_all_stuck_syncs` already did the work
   and had one caller. It is now a button that appears only when this sync's
   local job state is actually stuck, and the dispatch failure that used to name
   a command names the button instead.
3. *Blocking vs non-blocking issues.* One predicate (`blocking_issue_q`, with a
   row-level twin for rendering) now feeds baseline readiness, a Blocking column
   and a Blocking filter, so the list cannot disagree with the banner it
   explains.
4. *The full lists.* `out_of_scope` is the set the red Prune-orphans button
   deletes and the page showed 25 of it; `present_backfilled` had the same
   ceiling. Both are now full device tables over primary keys carried in the
   report.

**Plus the device-page ownership panel,** which explains a refused manual
delete. A customer deleting an uncovered device by hand got NetBox's raw
`ProtectedError`: three of our record names, no cause, no remedy. A second
attempt on a different device was refused by ten `netbox_routing` BGP rows
instead - rows this plugin does not own - which is why the panel reports the
database's verdict from `Collector.collect` rather than a count of our own
records, and says plainly when a blocker is one the prune cannot clear.

**And the support bundle carries what the panel shows.** The GUI now answers
the operator's questions; the bundle is what reaches us when the answer is
"this needs the vendor", and it carried the ingestion and merge jobs and
nothing about the scope panel, the operator buttons, or the ownership rows
behind a refused delete. Four sections were added - `scope_reconciliation`
(the stored report with name samples dropped and id lists reduced to counts),
`operator_action_jobs` (the last run of every button kind, which is where a
refused prune records what refused it), `ownership_records` and `stuck_sync` -
and the issue payload now marks each row blocking or not and exports the
blocking ones first, so the 200-row cap cannot truncate away the rows holding
the baseline back. Two more answer the questions a screenshot was the only
evidence for: `environment` (plugin, NetBox, branching and Python versions, plus
every optional integration with the version it was validated against and the
exact-app-set verdict) and `delete_blockers` - a capped survey over the devices
the cleanup targets, splitting the blockers this plugin releases itself from the
ones belonging to another plugin, which is the actual answer to "the uncovered
count is not going down". The bundle button now says on the page that this file
replaces screenshots.

## Validation

- `python3 scripts/check_operator_text.py` exits 0; it exited 1 on the tree
  before the rewordings, which is the evidence it can fail at all.
- `invoke ci` (the guard runs between `sensitive_check` and `harness_check`).
- Targeted: `test_button_jobs`, `test_scope_module_ui`, `test_issue_diagnosis`,
  `test_uncovered_absence_and_trend`.
- `scripts/validate_installed_routes.py` - it GETs every pk-only route in the
  release artifact and accepts only 200/302/405, so it is the check that
  cascades on a new `@register_model_view`.
- Full Django suite on an idle stack, then the same on `main` after the
  forward-port.

## Rollback

Every item is additive: two report keys, four views, two runners, one panel, one
column, one filter, one menu item, one guard script and its `ci` step. No
migration and no data change, so reverting the commits is sufficient and leaves
no state to clean up. A deployment that has already run the new
prune-uncovered button has deleted devices the previous release could delete
too, by command.

## Decision Log

- **The uncovered cleanup was CLI-only on purpose, and that was wrong.** The
  2.9.3 decision log argued that reading dry-run JSON is safer than clicking
  red. It put the fix out of reach of the person who reported the problem, who
  was looking at a panel showing a count and offering nothing to do about it.
  Reversed deliberately: danger is managed by gates, not by hiding the control.
- **No grandfather list in the guard.** Three strings named commands when the
  guard was written and all three were reworded in the same change. A
  grandfather list becomes permanent.
- **The device FKs stay `PROTECT`.** Making
  `ForwardDeviceIdentity.device` / `ForwardDeviceTagClaim.device` cascade would
  let a manual or merge-time delete drop ownership evidence without going
  through the audited release path - and that path is the only thing enforcing
  "another sync claims this device". The apply engine also relies on the
  resulting `ProtectedError` to record a skip rather than a failure. So the
  refusal stays and is explained on the device page instead.
- **`present_backfilled` primary keys are resolved by name.** The report knows
  those devices only by name, and a claim-based lookup would miss a device
  backfilled on its first run, before any claim exists. A duplicate name lists
  both rows, which is honest for a read-only list and is itself an already
  reported condition.
- **The uncovered prune was computing a protected tally and discarding it.**
  `_delete_prunable_devices` returns it and the orphan prune has reported it as
  `protected_by_model` since 2.5.5; the uncovered half dropped it on the floor,
  so a device refused by another plugin's rows reported a bare count and named
  nothing to act on. Now returned and recorded on the job.
- **The operator prune does not sweep foreign dependent rows, and the panel
  says so.** The sync-time prune sweeps the exact rows a `ProtectedError`
  reports and retries (2.5.5); `_delete_prunable_devices` does not - it
  releases this plugin's ownership and asks the database to delete the device.
  So a device held by ten `netbox_routing` BGP peers refuses the button too.
  Adding the sweep here would clear that device, and it would also mean the
  uncovered-prune button deletes other plugins' rows - a widening of a
  destructive path that needs its own allowlist and its own decision. Not
  taken in this change: the refusal now names the model, and the panel tells
  the operator to remove the foreign rows first. See Open.
- **No NetBox-model field on the issue filter form.** `model` is `FilterForm`'s
  own attribute for the model being filtered; a form field of that name shadows
  it. The search box already matches `model`.

## Open

- Phase 2: read-only report views for the eight audits with no GUI surface at
  all - primary-IP resolution, stale global IPAM counts, stale DLM notice count,
  apply-identity churn, ambiguous device names, dangling netbox-routing rows,
  APIC CIMC readiness and fast-baseline eligibility. `forward_interface_vlan_audit`
  additionally needs a decision on persisting interface primary keys.
- Phase 3: the docs and `SKILL.md` rewrite, the three panels the docs describe
  and no template contains, and extending `remediation_action` so every health
  check names a GUI control. Phase 3 follows Phase 2 deliberately: deleting a
  CLI instruction before its replacement exists makes the product worse.
- **Whether the operator prune should sweep foreign dependent rows.**
  Decided for this release: it does not. The sync-time prune sweeps; this one
  refuses, names the model that refused, and the device panel gives the
  operator the two-step remedy (delete the foreign records from their own
  plugin's pages, then run the prune). Revisiting it would need an explicit
  allowlist of what may be swept, the tally reported as `swept_by_model`, and
  tests pinning the negative space - the shape
  `destructive-paths-need-explicit-allowlists` requires. The residual gap is
  real and deliberate: a device held by another plugin's rows takes two
  operator steps rather than one.
- A per-device ownership release. The ownership panel explains the refusal and
  names the prune; it does not offer a way to delete one device by hand. That is
  a new destructive surface and is not in this change.
