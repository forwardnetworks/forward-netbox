# Tag delete-eligible global IPAM

**Date:** 2026-06-28

## Goal

Stamp network-global IPAM (prefixes, VLANs, VRFs) that a sync's latest Forward
fetch no longer reports with a self-healing `forward-delete-eligible` NetBox tag,
so an operator can filter by the tag and bulk-delete confirmed-stale objects by
hand. This is the safe first step toward IPAM scope cleanup (Partner's request):
tag-only, no deletion. Device-tag scope prune is device-derived and never
touches global IPAM, so these objects otherwise accumulate indefinitely.

## Constraints

- **Never deletes.** Tag-only. Operator reviews and deletes manually in NetBox.
- Self-healing: the tag set is reconciled to exactly the stale set on every run.
  An object that returns to the Forward fetch is untagged automatically (mirrors
  the `forward-backfilled` / `forward-out-of-scope` device-tag pattern).
- Empty-fetch guard: a model whose Forward fetch returns **zero** rows is skipped
  (reported under `skipped`), never tagged — an empty/failed fetch must not flag
  every NetBox object as eligible.
- Only objects with a determinate identity are tagged. `unmatchable` objects (no
  non-null lookup key) are never flagged stale, so are never tagged.
- Identity reuses the read-only audit's apply-engine key helpers, so a tagged
  object matches what the sync would consider the same object.

## Safety model (NetBox on_delete, introspected 2026-06-28)

A future auto-prune is bounded by NetBox itself — even a manual bulk delete is:
- VRF: `Prefix/IPRange/IPAddress.vrf` -> `PROTECT` (delete of a VRF with
  prefixes/ranges/IPs is refused); `Interface/VMInterface.vrf` -> `SET_NULL`.
- VLAN: `Prefix.vlan`, `WirelessLAN.vlan`, `VLAN.qinq_svlan` -> `PROTECT`;
  `Interface` untagged/qinq -> `SET_NULL`, `tagged_vlans` m2m cleared.
- Prefix: no PROTECT children; IPs relate by containment, not FK, so a prefix
  delete does not cascade-delete addresses.

## Touched Surfaces

- `forward_netbox/utilities/scope_ipam_audit.py` — `audit_model_rows` now returns
  `stale_pks`; add the `forward-delete-eligible` tag constants,
  `_apply_maintained_ipam_tag` (generic, model-agnostic), and
  `tag_delete_eligible_ipam`.
- `forward_netbox/management/commands/forward_tag_delete_eligible.py` — new
  management command wrapping the tag function (JSON output + remediation hint).
- `forward_netbox/jobs.py` — `tag_forward_delete_eligible_ipam` background job
  (builds client + SyncLogging, calls the tag function, stores result in job.data).
- `forward_netbox/views.py` — `ForwardSyncTagDeleteEligibleIpamView` (POST enqueues
  the job, `ipam.change_prefix` gated) + `tag_delete_eligible_ipam_url` in the
  Scope Reconciliation context.
- `forward_netbox/templates/forward_netbox/forwardsync_scope_reconciliation.html`
  — "Tag delete-eligible IPAM" card-footer button (always enabled; the eligible
  count needs live Forward fetches, so it is computed in the job, not on render).
- `forward_netbox/tests/test_scope_ipam_audit.py` — stale_pks, tag-stale-not-kept,
  self-healing untag, empty-fetch-skip.
- `forward_netbox/tests/test_scope_module_ui.py` — view enqueues job + job stores
  result (wiring test).

## Approach

Reuse `audit_model_rows`'s stale computation; the only new behavior is reconciling
a tag set against the stale PKs per model, inside one `transaction.atomic()`.
The tag is unrestricted by object type (matches the device-tag pattern), so the
same slug applies to all three IPAM models. The command mirrors
`forward_scope_ipam_audit` for selector/`--models`/`--limit` parsing.

## Validation

Unit: the four new tests above. Full suite. Lint/harness/sensitive. Manual live
run against the field sync to confirm the tag lands on the known-stale prefixes.

## Rollback

Revert the three surfaces. To clear tags already applied, delete the
`forward-delete-eligible` tag in NetBox (removes all assignments).

## Decision Log

- Tag-first over auto-delete: Partner asked to "start by tagging"; deletion of
  global IPAM is higher-risk (addressing data), so ship the review aid first and
  revisit auto-prune of the safe subset once the tag's catch is trusted.
- Tag slug `forward-delete-eligible` (over Partner's loose `delete_eligible`) to
  match the existing `forward-` tag-namespace convention.
- Empty-fetch skip is per-model, not all-or-nothing: a working fetch for VRFs
  should still tag stale VRFs even if the VLAN fetch came back empty.
