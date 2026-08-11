# Hold an ambiguous device name instead of refusing the tag domain

## Goal

Let ownership reconciliation complete when a device name resolves to more than
one NetBox device, so convergence unblocks and the drift report measures again.

This is the customer's live blocker on 2.7.9, named by their errored
`reconcile device scope tags (auto)` job as
`OwnershipConflictError: tag-mutation-identity-unresolved`, the one raise site
guarded by `if ambiguous:`.

## Constraints

- An ambiguous name must never be resolved to a guess: tagging the wrong device
  is worse than tagging neither.
- An ambiguous name must never lose a claim it already holds. `desired_ids`
  drives the release as well as the add, so simply dropping the key untags a
  device the plugin currently owns.
- A held claim must not be left at an older generation. `stale_claims` feeds
  `integrity_issue_count`, which gates `complete`, so a permanently stale claim
  would swap one convergence block for another.
- Device names are customer data. Only counts may reach a persisted diagnostic.
- The catalogue slug stays even though its raise site goes: job records
  persisted under it exist on customer systems.

## Touched Surfaces

- `forward_netbox/utilities/ownership.py` - `resolve_device_identities`,
  `reconcile_source_device_tag_claims`, `reconcile_sync_scope_tag_claims`
- `forward_netbox/utilities/vsys_parent.py` - resolution unpack
- `forward_netbox/utilities/scope_reconciliation.py` -
  `_apply_maintained_device_tag`, `tag_backfilled_devices`
- `forward_netbox/utilities/diagnostics.py` - catalogue comment only
- `forward_netbox/management/commands/forward_device_name_ambiguity_audit.py`
  (new, read-only)
- `forward_netbox/tests/test_absent_device_does_not_block_tag_domain.py`

## Approach

Two changes, in order of how much they matter.

**Narrow what counts as ambiguous.** A candidate this sync already maps to a
different source key is that other Forward device. The `(sync, device)`
uniqueness constraint means binding a second name to it could never have
succeeded, so excluding it is not a tie-break - it removes a candidate that was
never eligible. This is expected to resolve the tie outright in the common
shape: NetBox scopes device-name uniqueness to the site, so a device that moves
site or is re-created alongside its predecessor leaves exactly two rows, one of
them already spoken for.

This exclusion forces a second distinction: `missing` must mean NetBox has no
device of that name AT ALL, not merely no bindable one. A device whose Forward
name changed still carries its old identity row, so under the new name every
candidate is excluded here - and calling that "missing" would drop it from
`desired_ids` and strip its Forward tags silently on every run. A name with
rows but no bindable one is held, not skipped.

**Hold what remains.** Where a genuine tie survives, the name is neither added
nor released, and its existing claims are refreshed to the current generation.
The device keeps exactly the tag state it already had. The counts
(`ambiguous_device_names`, `held_ambiguous_devices`) are returned and surfaced
so a growing tie is visible rather than silent.

The hold covers only names present in the current mutation. A name Forward no
longer reports is not held by anything, ambiguous or not - otherwise a device
that genuinely left scope could never be untagged.

The count alone is not actionable, so `forward_device_name_ambiguity_audit`
names the devices behind it. That split is deliberate: the reconcile result is
persisted into a job record, where device names are customer data and only
counts may go; the audit runs on the operator's own console against their own
NetBox, which is the one place the names may be shown.

## Validation

`invoke ci`, plus the rewritten
`test_absent_device_does_not_block_tag_domain`, which now pins: the tie is not
claimed on a guess, an existing claim survives the hold, the held claim is
refreshed to the current generation, the domain reaches COMPLETED, a departed
name is still released, and a tie whose twin belongs to another source key
resolves to the one free candidate.

## Rollback

Revert. Ambiguity returns to refusing the whole domain, and the customer
returns to ownership never completing.

## Decision Log

- **Hold rather than refuse.** Refusing an entire domain to protect one name is
  the same all-or-nothing mistake 2.7.9 fixed for absent names, made twice
  against the same customer. The protection ambiguity needed was never
  domain-wide; it was per-name, and per-name is what it gets.
- **Refresh held claims.** Holding without refreshing looks safer and is not:
  it leaves `integrity_issue_count` permanently non-zero, which is the exact
  mechanism that keeps drift reading "Not measured".
- **Exclude already-mapped candidates rather than prefer them.** Preferring one
  candidate is a guess with a heuristic attached. Excluding a device that the
  uniqueness constraint already forbids is not a preference at all.
- **Keep the slug with no raise site.** A catalogue that only describes
  currently-reachable code cannot decode a job record written last week.

## Open

- Whether the customer's 49 out-of-scope devices are genuinely absent from the
  Forward tag scope result is a separate question, unaffected by this change.
  Out-of-scope membership is decided purely by absence from the query result,
  so it must be confirmed in Forward before anything is pruned.
