# The `forward-uncovered` device tag

## Goal

Make the "carry no include tag" bucket enumerable. An operator seeing a count
there must be able to list the devices behind it, the same way the other two
scope-reconciliation buckets already can be listed.

## Why

A customer reported 552 devices under "carry no include tag" while "out of
scope (orphans)" read 0, and asked which devices they were. Nothing could
answer him:

- The panel showed a count and, on the actionable half, a badge linking to
  `/dcim/devices/?tag_id__n=&q=`. Both parameters are empty, so NetBox ignores
  them and the link opens the unfiltered device list.
- `owned_untagged_sample` and `unclaimed_sample` were computed and returned in
  the payload, and then never rendered. Every other bucket on the page renders
  its sample; this one did not.
- The CLI audit is not a fallback: it prints the same payload, so it is capped
  at the same `SAMPLE_LIMIT = 25`.

The count is also the one that grows, which is why it was the count being
asked about. A device disabled in Forward vanishes from `network.devices` and
from the REST inventory alike, so it drops out of the tag-scope result; since
the absence quarantine landed in 2.8.0 it is deliberately kept rather than
pruned. It therefore accumulates here, permanently, and part of the growth the
customer is seeing is the 2.8.0 fix working as designed.

Orphans staying at 0 while this climbs is not a contradiction. An orphan is a
device this sync PREVIOUSLY CLAIMED and no longer sees, and a claim is released
on the first run that observes the absence. A device this sync created but never
held a scope claim for was never an orphan of it.

## Approach

The other two buckets are answerable for exactly one reason: a maintained tag
is applied to them, so `?tag=forward-backfilled` lists every member. The gap is
that the third bucket has no such tag, and the empty href is the symptom rather
than the defect. So close it the same way instead of a new way.

`forward-uncovered` is maintained by `tag_backfilled_devices()` alongside the
other two, from `report["_owned_untagged"]`.

Deliberately NOT applied to the `unclaimed` half. A device this sync never
created is not this sync's to label. That is enforced rather than trusted: an
unclaimed device holds no `ForwardDeviceIdentity`, so it resolves as `missing`
and is skipped by the claim machinery on its own.

## Touched Surfaces

- `forward_netbox/models.py` - `UNCOVERED` on BOTH `ClaimType` enums
  (`ForwardDeviceTagClaim` and `ForwardManagedDeviceTag`).
- `forward_netbox/utilities/ownership.py` - `STATUS_CLAIM_TYPES` and
  `_NEGATIVE_STATUS_CLAIM_TYPES`, replacing three inline literal pairs.
- `forward_netbox/utilities/scope_reconciliation.py` - the tag constants, the
  `_owned_untagged` name set, and the third `_apply_maintained_device_tag` call.
- `forward_netbox/utilities/tag_contracts.py` - reserve the new slug.
- `forward_netbox/views.py` + the scope-reconciliation template - the working
  link and the two sample cards.

No migration. Django 6.0 does not treat a `choices` change as schema state;
`makemigrations --check` exits 0 against these edits.

## Constraints

- The tag says a device is not covered BY THIS SYNC. It must never be published
  as a deployment-wide verdict, so it carries the same cross-sync subtraction
  `out_of_scope` does.
- It is not a delete gate. Nothing prunes on this tag, and the absence
  quarantine remains the only thing that authorizes a deletion.
- `Tag.description` is 200 characters, and the write raises rather than
  truncating.

## Validation

- `forward_netbox/tests/test_uncovered_device_tag.py` - 13 tests: what the tag
  is for, the not-an-orphan shape the customer actually has, both directions of
  idempotency, the unclaimed exclusion, the backfilled non-overlap, the
  cross-sync subtraction, and the four contracts a fourth status tag would have
  to satisfy.
- Adjacent suites: `test_ownership`, `test_scope_module_ui`,
  `test_device_scope_reconciliation_audit_command`, `test_ownership_migration`,
  `test_reserved_status_tag_adoption`, `test_ownership_conflict_reason`,
  `test_health`.
- Full Django suite.

## Rollback

Revert. The tag stops being maintained and any assignments it left behind are
inert - no other code reads it, and the panel counts are computed from the
Forward result, not from the tag.

## Decision Log

- **A tag, not a filter parameter.** The badge could have carried an explicit
  `?id=` list, but at 552 devices that is a 5 KB URL that goes stale the moment
  the next reconciliation runs. The tag is recomputed by the same job that
  computes the count, so the link and the number can never disagree.

- **`owned_untagged` only.** Tagging the whole bucket would have matched the
  552 the customer quoted, which is superficially what he asked for. It would
  also have this sync asserting something about devices another source owns.
  The split already exists precisely because the two halves need different
  treatment; collapsing it to make one number match would undo that.

- **The claim types are named, not derived.** `_NEGATIVE_STATUS_CLAIM_TYPES` is
  an explicit tuple rather than "every status type except backfilled". Getting
  the membership wrong is silent in both directions - a missing type means the
  tag is never applied, an extra one means it lands on devices that are fine -
  so the set is spelled out and pinned by a test.

- **Both `ClaimType` enums.** There are two, on two models, and only one of them
  is the claim table. Adding to one and not the other leaves the managed-tag
  registry unable to represent the tag it is asked to materialize. A test now
  pins them equal.

- **Resolving these names can retire a stale identity binding, and that is
  correct.** `reconcile_source_device_tag_claims` resolves by device NAME, and
  an owned-untagged device may hold an identity under a different Forward key -
  the rename case. The retirement only fires when the bound key is absent from
  `live_source_keys`, i.e. when Forward no longer reports that name anywhere. A
  device merely renamed in NetBox while Forward still reports the old key is
  held instead, because the key is still live. So the new call site cannot
  retire a binding that is still in use; it can only finish a retirement the
  rename path already intended.

- **No health signal in this change.** "It keeps growing" is the customer's
  actual complaint, and `_out_of_scope_summary` shows how a trend signal on this
  count would be built. It is a separate change: this one makes the bucket
  answerable, and a warning about a number nobody could enumerate would have
  been the wrong order to do it in.

## Limits

- ~~The `unclaimed` half stays un-enumerable beyond its 25-name sample.~~
  **Closed in this same release.** Both halves list in full:
  `forwardsync_unclaimed_devices` and `forwardsync_uncovered_devices`
  (`views.py:1545,1551`), reached by the "List all N" buttons on the scope
  panel. The limit was written against the sample-only version and never
  updated when the device ids were kept for exactly this purpose
  (`scope_reconciliation.py`, `unclaimed_device_ids`).
- The tag reflects the most recent reconciliation run, not live Forward state.
  A device re-enabled in Forward keeps the tag until the next run.
- Nothing back-fills the tag onto devices whose absence predates this change
  until the next reconciliation runs. This resolves itself on the next run; it
  is a timing note, not an outstanding defect.
