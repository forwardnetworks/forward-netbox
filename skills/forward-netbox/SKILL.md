---
name: forward-netbox
description: >
  Operate the Forward Integration for NetBox plugin (`forward_netbox`) — syncing Forward
  Networks inventory into NetBox. Covers the ingestion pipeline (branch, stage, merge,
  promote), device tag scope and what "out of scope" really means, the orphan prune and
  every guard in front of it, the ownership tables, NQE query mechanics, and the read-only
  audit commands. Use when configuring a sync, reading the Scope Reconciliation panel,
  diagnosing devices that appear or vanish unexpectedly, or before anything that deletes.
license: MIT
---

# Forward Integration for NetBox

`forward_netbox` syncs Forward Networks inventory into NetBox: devices, interfaces, IP
addresses, MAC addresses, inventory items, modules, cables, plus optional integrations
(lifecycle/CVE data, routing) when those plugins are installed.

It is a **field integration reference**, not an officially supported Forward Networks
product.

> **Check the installed version before trusting any specific behaviour here.** This plugin
> changes quickly and several defaults in this document arrived recently. `pip show
> forward-netbox`, or the plugin's version on the NetBox plugins page.

## Retrieval Sources

| Source | Where | Use for |
|--------|-------|---------|
| Repository | `github.com/forwardnetworks/forward-netbox` | Code, `CHANGELOG.md`, plans under `docs/03_Plans/` |
| User guide | `docs/01_User_Guide/` in the repo | Configuration, operations |
| Live instance | The Sync detail page and its Scope Reconciliation panel | The authoritative current state |
| Support bundle | Sync detail → support bundle | Diagnosis without shell access |

## The Pipeline

A run moves through distinct stages, and knowing which one failed is most of a diagnosis:

1. **Fetch** — NQE queries run against a Forward snapshot. Read-only.
2. **Validation** — blocking checks. A failed device query blocks dependent models rather
   than letting a partial estate through.
3. **Staging** — rows are written inside a `netbox_branching` branch, never directly to
   main tables.
4. **Merge** — the branch merges into NetBox proper.
5. **Baseline promotion** — the contributor baseline records what each contract returned,
   which is what later runs compare against.

**Branching is mandatory.** Do not propose writing directly to NetBox tables to make a
sync faster. Staging inside a branch is what makes a bad run reviewable and abortable, and
the overwhelming majority of sync time is inside the branch by design.

**A failed row blocks baseline promotion.** No attestation means no bookkeeping, which is
why drift then reports "Not measured". The escape hatch is
`forward_accept_merge_failures`, which is deliberate and audited — reach for it knowingly.

## Scope: the concept most often got wrong

Scope is defined by **Forward device tags** (include tags, optional exclude tags, and an
any/all match). Everything downstream follows from the set of devices the current query
returns.

**"Out of scope" means absent from the current query result. That is absence, not
evidence.** A device can be missing because:

- it genuinely left the estate;
- someone edited its tags in Forward;
- the query narrowed, or partially failed;
- **it was disabled in Forward.**

These are indistinguishable in the result, so the Scope Reconciliation panel classifies
them where it can (gone from Forward / in Forward but untagged / custom-command source).
**Confirm in Forward before acting on any of them.**

### Disabled in Forward looks exactly like deleted

A device disabled in Forward vanishes from `network.devices` *and* from the REST
inventory. The plugin has no interface that can tell it from a decommissioned device. This
one condition produces three symptoms at once, which look like three separate bugs:

- no include tag applied;
- no software version (device-derived data has no device to hang from);
- counted as uncovered, and eligible for the prune.

Devices in this state often still carry feature tags from when they *were* collected,
which makes them look managed. If someone reports "this device should be covered but
isn't", check whether it is disabled in Forward before anything else.

### Two maintained tags

- `forward-backfilled` — tagged in scope, but not freshly collected in this snapshot. Kept
  on purpose.
- `forward-out-of-scope` — matches none of the include tags. These are the removable
  orphans.

Both are idempotent: after a run each tag's device set matches its bucket exactly, so
`/dcim/devices/?tag=forward-out-of-scope` is a reliable filter.

## Deletion, and the guards in front of it

`device_tag_prune_out_of_scope` deletes NetBox devices absent from the Forward scope.
Operators enable it deliberately. Before proposing or running it, know all four guards:

1. **Empty-scope refusal** — a query returning zero devices refuses outright, because
   every NetBox device would otherwise read as an orphan.
2. **Shrink guard** — an orphan set that is a large share of what the sync previously
   claimed is refused as a likely query or tag fault. Overridable.
3. **Absence quarantine** — an orphan must be absent across several consecutive syncs
   *and* for a stretch of wall-clock time before it is prune-eligible. This exists because
   disabling is usually temporary and deletion never is. Configurable per source; the
   manual button can override it, a scheduled run cannot.
4. **Ownership release** — a device claimed by another sync, or carrying customer-owned
   tag assignments, is not deleted.

**Rules for any deleting change:** name the allowlist of what may be deleted, state which
gate you are not bypassing, and test the negative space — what must survive. "Every model"
has shipped twice here and deleted customer devices unattended both times.

## Ownership

The plugin records what it owns so it can safely release it later:

| Table | Records |
|-------|---------|
| `ForwardDeviceIdentity` | This sync created this NetBox device, under this Forward key |
| `ForwardDeviceTagClaim` | This sync applied this tag (scope / backfilled / out-of-scope) |
| `ForwardDeviceAbsence` | How long a device has been missing, for the quarantine |
| `ForwardVirtualParentClaim` | Virtual device → physical parent |
| `ForwardOwnershipReconciliation` | Per-domain evidence that ownership is current |

Two consequences worth knowing:

- **Untagged devices split by owner.** "Carries no include tag" covers a device this sync
  created (ours, worth investigating) and one it never claimed (not ours to reason about —
  another source, an operator, or a leftover). Orphans can read zero while hundreds are
  untagged, because a device the sync never claimed is not an orphan of it.
- **An ambiguous device name is held, not refused.** Names resolving to more than one
  NetBox device keep their existing tag state; the count is reported so the duplicates can
  be cleaned up.

## NQE mechanics

- Execution is **asynchronous**: submit, poll, then page the result. A single unpaginated
  fetch silently truncates.
- **Every NQE run costs.** Treat call volume as a budget: reuse a report the sync has
  already computed rather than re-querying, and audit `forward_api_usage` when changing
  anything on the sync path.
- The query language is restrictive — no list comprehensions, and `contains` is not
  infix. Write queries device-parallel.
- Queries can be pinned by ID or resolved by name; a pinned ID going stale is a real
  failure mode the health panel warns about.

## Read-only audit commands

All are diagnostics and none delete. Run them before drawing conclusions:

| Command | Answers |
|---------|---------|
| `forward_device_scope_reconciliation_audit` | Which devices are in scope, backfilled, or orphaned |
| `forward_ownership_audit` | Is ownership evidence current |
| `forward_apply_identity_audit` | Why objects churn (created and deleted every run) |
| `forward_device_name_ambiguity_audit` | Which names resolve to several devices |
| `forward_collection_gap_alert` | Devices Forward failed to collect, and why |
| `forward_blocker_audit` / `forward_warning_audit` | What is blocking or warning, with reasons |
| `forward_scope_ipam_audit` | Global IPAM objects the sync no longer covers |
| `forward_primary_ip_audit` | Unresolved primary-IP assignments |
| `forward_interface_vlan_audit` | Interfaces failing on cross-site VLAN rejection |
| `forward_dlm_hardware_notice_audit` | Lifecycle notices Forward no longer emits |
| `forward_module_readiness` | Module bays that must exist before modules sync |

Prefer the UI equivalents where they exist — operators should not need a shell.

## Diagnosing common reports

**"Devices were deleted that shouldn't have been."** Check `prune_out_of_scope`, then
whether the devices were disabled in Forward, then whether the scope query narrowed. The
prune reports what it held back and why.

**"Drift shows Not measured."** Baseline promotion did not complete — usually a failed row
in the merge. Fix the row or accept the failures deliberately.

**"A device should be covered but isn't."** Disabled in Forward, collection failed, or the
tag was never applied in Forward. `forward_collection_gap_alert` separates the first two.

**"The same objects are created and deleted every run."** Identity churn: Forward and
NetBox compute different keys for the same object. `forward_apply_identity_audit` names
them.

**"The sync deleted a huge number of rows in one run."** Look at what the query returned
before assuming a bug — a narrowed or partially-failed query manufactures removals out of
valid data. Guards exist precisely because this is not hypothetical.

## Safety rules

- Never propose bypassing branching for speed.
- Never widen a deletion path to "every model" — name the allowlist.
- Treat any count that would delete customer data as needing confirmation against Forward
  first, not just against NetBox.
- Persisted diagnostics carry schema identifiers only, never customer data. Keep it that
  way when adding any.
