# 2.5.5 — prune protected-reference sweep + push-triggered sync (webhooks)

Status: implemented on `fix/2.5.5-prune-protectederror`.

## Goal

Second item (maintainer-added mid-release): **push-triggered sync**. The
operator wants Forward to push a webhook that starts a sync instead of polling
on a schedule. The NetBox-native inbound path already exists
(token-authenticated `POST /api/plugins/forward/sync/<id>/sync/`) and the docs
now lead with it; a new `POST .../webhook/` action covers senders that cannot
set an `Authorization` header: per-sync `webhook_secret` (sync form, Execution
fieldset; empty disables), `X-Forward-Webhook-Secret` header or `?secret=`
fallback, `hmac.compare_digest`, one opaque 403 for every failure cause,
already-running acknowledged with 202 and no re-queue (webhook-retry
idempotent), SyncError mapped to 409, job attributed to the sync's configured
user (never anonymous). Tests exercise the real router URL so the action's
`authentication_classes=[]`/`permission_classes=[]` initkwargs are proven in
the production wiring. Docs: Operations Guide "Push-triggered sync (webhooks)".

First item (the original 2.5.5 driver):

Field report: the "prune orphans" job errored with
`ProtectedError: Cannot delete some instances of model 'Device' ...
'Interface.device'` listing netbox_routing **BGPPeer** objects, and pruned
**nothing**. Root cause: `prune_orphan_devices` deleted devices with a raw
`Device.objects.filter(pk__in=batch).delete()` inside a single transaction —
bypassing the sync path's delete adapters and `DELETE_DEPENDENCY_MODEL_ORDER` —
and relied on Django's cascade. netbox_routing's `BGPPeer.peer`/`BGPPeer.source`
hold `on_delete=PROTECT` FKs to the IP addresses on a pruned device's
interfaces, so the cascade collector raised and the whole prune rolled back.

## Constraints

- Plugin-agnostic: must work with netbox_routing / peering-manager / ACI or any
  future plugin's PROTECT edge, with or without the plugin installed — so no
  plugin imports, no per-plugin selector maps.
- Must not delete plugin rows that reference only in-scope devices.
- One stuck batch must not roll back the rest of the prune.
- The sync-time delete path already orders deletes children-first and converts
  ProtectedError into a recorded dependency-skip — unchanged.

## Touched Surfaces

- `forward_netbox/utilities/scope_reconciliation.py` — new
  `_delete_devices_sweeping_protectors` + `_group_protected_objects_by_rank`;
  `prune_orphan_devices` uses them and returns `pruned_dependent_rows`.
- `forward_netbox/jobs.py` — prune job surfaces `pruned_dependent_rows`.
- `forward_netbox/management/commands/forward_device_scope_reconciliation_audit.py`
  — `--prune-orphans --apply` output includes the sweep tally.
- `forward_netbox/tests/test_device_scope_reconciliation_audit_command.py` —
  new `PruneProtectorSweepTest`.

## Approach

Catch-sweep-retry: per 500-PK batch (now one transaction **per batch**), try the
device delete; on `ProtectedError`, group `exc.protected_objects` by model,
order children-first via `DELETE_DEPENDENCY_MODEL_RANK` (unknown models last),
delete each group by PK (a nested ProtectedError just defers that layer to the
next pass), and retry — capped at 20 passes, then re-raise with the sweep tally
for diagnosability. Django hands us exactly the blocking rows, so in-scope-only
plugin rows never appear; a blocker owned by an in-scope neighbor whose FK
targets a pruned device's IP is correctly swept and recreated by the next sync.

Rejected: pre-deleting via per-model ORM selectors (new per-plugin selector
surface, an 18th optional-plugin touchpoint) and reusing sync delete adapters
(they need Forward row dicts; the prune has only PKs).

## Validation

- CI-safe tests (no plugins needed — the sweep never imports plugin models):
  grouping/order pure function (children-first + unknown-last), sweep-and-retry
  with real stand-in rows via a patched Device manager (blockers deleted, retry
  succeeds, tally correct), cap re-raise with "sweep passes" message. Existing
  prune tests (dry-run, empty-scope refusal, apply, identity PKs) green.
- 12/12 module tests pass; full suite + lint + harness gates.
- Live validation: run the prune once on 2.5.5 — expect SUCCESS with a nonzero
  `netbox_dependent_rows` netbox_routing.bgppeer count; second run prunes 0.

## Rollback

Single squashable branch; revert the merge commit. The sweep only activates on
a ProtectedError that previously failed the job outright, so reverting restores
the old failure mode and nothing else.

## Decision Log

- **Sweep from `exc.protected_objects` (option b)** over dependency-ordered
  pre-delete (option a): plugin-agnostic for free, no new per-plugin selector
  maps, covers future PROTECT edges without touching the optional-plugin
  checklist.
- **Per-batch transactions**: the field failure was all-or-nothing rollback;
  batch isolation bounds the blast radius of any future edge.
- **Deliberate cross-device sweep**: an in-scope neighbor's BGPPeer pointing at
  a pruned device's IP must be deleted for the prune to proceed; the next sync
  recreates it from Forward data (verified claim in the 2.5.4 investigation).
- Dangling GenericFK rows (BGPRouter/Scope/Setting) don't block deletes; a
  guarded cleanup is deferred (2.5.6/2.6 candidate).
