# 3.0.1: forward-port the 2.9.x lane onto main

## Goal

`main` (NetBox 4.7 only, netbox-branching 1.2) carries every feature and fix
that shipped on `maint/2.9.x` from 2.9.4 through 2.9.6, adapted to 4.7,
with the optional plugins that can now run on 4.7 admitted by the runtime
validator, cut as 3.0.1.

## Constraints

- No upstream changes to any optional plugin.
- Every ported change keeps main's own 4.7 adaptations intact: the ltree
  tree-model detection in `bulk_merge.py`, `enforce_sync_job_safety` in
  `signals.py`, the mptt-to-ltree and `CustomField.get_for_model` shapes in
  `fast_baseline.py`, the `change_control` package and its views, and the
  4.7 / 1.2 series pins in `validated_runtime.py`. Hunks are diffed and
  re-applied; no file is overwritten wholesale from the 2.9.x lane.
- Migrations take fresh numbers after main's `0055`; the
  `ForwardNQEMap.netbox_model` choice migrations are regenerated against
  main's `choices.py` rather than transplanted, because their literals
  encode the whole choice set and main's set is smaller. Every migration
  depends on `dcim.0001_initial`, never a version-specific dcim migration.
- Published package metadata carries no direct git URL: PyPI refuses a
  distribution whose requirements do, and that would refuse the 3.0.1 tag.
- ACI maps stay seeded disabled until netbox-cisco-aci raises its
  `max_version` past 4.6.99.
- No customer names, identifiers, or scan-matching hostnames in committed
  content, commit messages, or pull-request bodies.

## Touched Surfaces

Phase 0 (this pull request): `pyproject.toml`, `constraints.txt`,
`development/constraints-upgrade-from.txt`, `development/Dockerfile`,
`development/configuration/plugins.py`, `poetry.lock`,
`forward_netbox/utilities/validated_runtime.py`,
`forward_netbox/utilities/plugin_integrations/registry.py`,
`forward_netbox/tests/test_optional_plugin_versions.py`,
`forward_netbox/tests/test_plugin_integrations.py`,
`docs/03_Plans/active/2026-09-02-netbox-4.7-runtime.md`.

Later phases: the union of the ported commits' file lists - `models.py`,
`choices.py`, `views.py`, `urls.py`, `navigation.py`, `template_content.py`,
the sync, drift-comparison, scope-reconciliation, fast-baseline, workload-
state and bulk-merge utilities, new `sync_routing_policy.py`,
`audit_reports.py`, `routing_dangling_audit.py`, `apic_cimc_readiness.py`,
the routing-policy and ACI `.nqe` queries, templates, migrations `0056`
through `0059`, their tests, and the user guide.

## Approach

**Why the scope is the whole lane, not one release.** `main` forked from the
2.9.x line before v2.9.4 was tagged; migration lineage is shared only
through `0052`. The 2.9.3 fixes were already carried over (#377) and
netbox-dlm 0.10.0 was already re-admitted (#364). Everything else the 2.9.x
lane added afterwards never reached main - nothing was removed on main, so
the port is purely additive.

**Phase 0 - dependencies and the runtime validator (this pull request).**
Four of the five optional plugins now run on 4.7. netbox-validity 3.6.0 is
the first release to declare it (3.5.2 declares only 4.4-4.6).
netbox-peering-manager 0.3.1 carries its own 4.7-readiness work (its
published badge is stale). netbox-routing's 4.7 support is merged on its
upstream `main` (reported as 0.4.4) but not tagged, so the development and
CI image installs it from that branch archive; the published constraint is the
range `>=0.4.3` so the distribution stays uploadable. The constraint files
carry no routing pin at all while it comes from the branch archive: they are fed to
`pip-audit`, which resolves from PyPI and fails on a version it cannot find.
The runtime validator holds the installed version to exactly `0.4.4`
instead, failing closed if the archive ever reports anything else.
netbox-cisco-aci is unchanged: its upstream still declares
`max_version = "4.6.99"`, so it cannot boot here and stays out of
`VALIDATED_PLUGIN_APPS`. The validator's app set and distribution table
gain the three returning plugins; the registry's per-plugin version fields
and both test fixtures follow.

**Phase 1 - 2.9.3.** Already on main (#377); nothing to do.

**Phase 2 - 2.9.4.** Every operator diagnostic and remediation reachable in
the GUI: `describe_delete_blockers`, the gated prune buttons, the stuck-sync
recovery control, the Blocking column, the listable device sets, the device
ownership panel, the support bundle.

**Phase 3 - 2.9.5, four pull requests in order.** The numbers an operator
reads are current, honest and actionable; prefix lists, community lists and
route maps from device configuration (`sync_routing_policy.py`, migration
`0056`); the credentialed config-backup path and its operator button; ACI on
a real fabric with the five tenant-policy maps (migration `0057`).

**Phase 4 - 2.9.6, four pull requests in order.** An issue row keeps its own
message; a manual device delete releases the plugin's own ownership
records with the badge, card and drift-report fixes (migration `0058`); the
fast baseline stays on with ACI maps and the ACI attachment maps
(migration `0059`); route-map entries link to the lists they match and the
nine operator audits become pages on the sync.

**Phase 5 - release 3.0.1**, including whether `scripts/release_lane.py`
comes to main so the two lanes stop carrying divergent release scripts.

## Validation

- Every phase: the ported commit's own tests plus `invoke ci` green on the
  4.7 stack before its pull request opens; migrations applied on a fresh
  database and on one migrated through main's `0055`.
- Phase 0: the 4.7 image boots with all four admitted plugins in `PLUGINS`;
  the routing, peering-manager and validity tests that skipped on this
  runtime now run; the runtime decision still selects COPY/SQL, the
  set-based merge and the fast baseline with them installed.
- Routing phases: a sync against the validation org with the routing-policy
  maps enabled converges on the second run.
- ACI phases: tests only - the maps cannot be exercised live on 4.7 until
  netbox-cisco-aci raises its ceiling.

## Rollback

Each phase is its own pull request and its own migrations, revertible
independently while nothing later depends on it. Phase 0 is a dependency
and data-table edit with no migration. The choice-table migrations are
`AlterField`s, reversible with no data repair. ACI maps ship disabled, so a
revert there is invisible to a running deployment.

## Decision Log

- **Full catch-up, one plan.** Chosen after seeing that the 2.9.6 items
  depend on whole 2.9.4 and 2.9.5 features that never reached main, and
  that the real gap is the lane, not a release.
- **Port in release order.** That is the dependency order: the 2.9.6
  device-delete work calls 2.9.4's `describe_delete_blockers`; the ACI
  attachment maps build on 2.9.5's tenant-policy maps; the route-map links
  build on 2.9.5's routing-policy sync; the audit pages are phase two of
  2.9.4's GUI work.
- **netbox-routing follows upstream `main` in the image, not in the
  package metadata.** A direct URL in `Requires-Dist` is refused by PyPI;
  the range in the metadata, the branch archive in the image, and the exact
  version in the runtime validator give CI the upstream branch without
  making the distribution unpublishable or the audit unresolvable.
- **The validator's app set is an exact match in both directions**, as on
  the 2.9.x lane. Admitting three plugins means a deployment without them
  takes the safe slow paths; that is the existing policy, and the fast
  paths are only trusted against the runtime they were validated on.
- **netbox-cisco-aci code is ported, maps stay disabled.** The day upstream
  lifts its gate, a 3.0.x deployment only flips maps on.

## Progress

- 2026-09-15: Phase 0 built; `invoke ci` green.
- 2026-09-15: the customer's first-day-on-2.9.6 fixes carried across
  ahead of the rest of the port (they are being fixed on `maint/2.9.x`
  as 2.9.7 and belong on both lanes): a per-model progress total that
  grew in installments as a split model's plan items applied, so the bar
  ran backwards - `initialize_plan_statistics` now sums every item up
  front; SNMP endpoints whose walk returned neither `sysDescr` nor
  `sysObjectID` reading Unknown when Forward's own profile assignment
  named the vendor - the endpoint branch now reads the profile name; and
  a blocking failure that named the models but not the reason -
  `describe_failure` appends every failed model's already-safe reason
  slug, generically, and `safe_exception_summary` keeps the full sentence
  for the two reasons only the plugin's own map-enablement message ever
  raises.
- 2026-09-15: from the same failure, `ForwardSyncForm.clean` now refuses
  to save a sync whose enabled model has no enabled NQE map, with the
  run's own sentence on the offending checkbox; that sentence moved from
  the query fetcher into `query_registry.missing_query_specs_message` so
  the form and the run cannot drift. (Maps are global, so the checkbox
  cannot enable one - refusing the save is the honest alternative.)
- 2026-09-15: a fifth 2.9.7 item landed on `maint/2.9.x` -
  `release_foreign_delete_blockers`, releasing exactly the allowlisted
  `netbox_routing` rows blocking a manual device delete, offered as a
  button on the device ownership panel - but **NOT carried here**. It
  builds directly on `describe_delete_blockers` and
  `ForwardDeviceOwnershipPanel`/`device_ownership_panel.html`, which are
  2.9.4's/2.9.6's ownership-panel work (Phase 2's `bc73e57` and Phase 4's
  `da9457f`) and do not exist on `main` yet - `template_content.py` here
  is still the 967-byte pre-ownership-panel version. Porting this item
  now would mean pulling forward a slice of Phase 2/4 early and out of
  the planned dependency order. Carry it forward as part of Phase 2/4
  instead of as a 2.9.7 patch: see `maint/2.9.x` commit `c2f61a5` for the
  full change to re-apply once `describe_delete_blockers` exists here.
