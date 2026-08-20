# A change batch carrying two models corrupted ChangeDiffs, then crashed the sync

## Goal

Fix the crash that cost a deployment three syncs, at its cause.

## Why

The failure presented as an unhandled `KeyError` thirteen seconds into
`ipam.ipaddress` plan item 51 of 90, deterministically, across three runs, two
plugin versions and two NetBox versions - with nothing in any record naming a
key or a line.

The cause, each link verified rather than inferred:

1. The ipaddress write block emits ONE `updated` list carrying two models: the
   IPAddresses it wrote plus the Devices whose primary-ip claims those IPs
   released (`released_primary_devices`).
2. `_sync_branch_change_diffs` typed the whole batch from
   `object_changes[0].changed_object_type` and matched existing ChangeDiffs by
   bare `object_id`. The Device changes were therefore filed against
   IPAddress-typed diffs, and diffs were created whose `object_type` said
   IPAddress while their `original` held a serialized Device - the devices are
   `snapshot()`-ed by `release_owned_primary_ip_claims`, so that `original` is
   real, not None.
3. NetBox uses per-table pk sequences, so a Device pk colliding with an
   IPAddress pk is ordinary - the deployment has roughly 4k devices against
   39k IPs.
4. A later ipaddress shard updated the IP with the colliding pk, matched the
   corrupted diff, and Branching's `_update_conflicts` - which iterates
   `original`'s keys and indexes `modified` directly
   (`netbox_branching/models/changes.py`) - raised `KeyError` on the first
   field one model has and the other does not.

Reproduced both ways on the unfixed code. With the IP's own diff first the key
is `vrf`. With the mixed batch first, the batch also creates a SECOND diff
under the same (type, object_id) carrying Device content, and the key is a
Device-only field - not sync-contract vocabulary, which is why the release
that made contract keys nameable still reported nothing for that deployment.

Why item 51 specifically: shard boundaries are content-determined, so the same
data put the same colliding row in the same shard every run. Why thirteen
seconds in: classification precedes the write block, and the emit runs at its
end.

## Constraints

- Fix at the sink. Fixing only the mixed call site leaves every future caller
  one refactor away from reintroducing silent corruption.
- No change to what a sync writes to NetBox objects. This is bookkeeping about
  changes, not the changes.
- The reproduction must fail on the unfixed code, or it proves nothing.

## Touched Surfaces

- `forward_netbox/utilities/apply_engine_bulk.py` -
  `_sync_branch_change_diffs` groups by each change's own
  `changed_object_type`
- `forward_netbox/tests/test_mixed_model_change_diffs.py` (new)
- `docs/03_Plans/active/2026-08-19-release-2.8.6.md` - scope amended
- `.pre-commit-config.yaml`

## Approach

`_sync_branch_change_diffs` partitions its batch by `changed_object_type` and
recurses per group; the single-type path is unchanged. Callers stay as they
are - the ipaddress site's mixed emit becomes correct rather than forbidden.

Every other emit call site was audited: all single-model.

## Validation

`test_mixed_model_change_diffs.py`, run in an isolated runtime on NetBox 4.6.8,
with the pk collision forced explicitly:

- Fixed: 2 tests OK. The device gains a Device-typed diff, the IP keeps its
  own, and each diff's content matches its own model.
- Unfixed (negative control, fix reverted for the run): both tests error -
  `KeyError: 'vrf'` in one direction, and duplicate same-identity diffs with
  Device content in the other.

Full Django suite before landing.

## Rollback

Revert. The crash returns for any deployment whose released-primary devices
share a pk with a later-shard IP.

## Decision Log

- **Group at the sink, not the call site.** The call site's mix is legitimate -
  those devices genuinely changed in this transaction - and a sink that only
  works for homogeneous batches is an undocumented precondition that already
  failed silently once.
- **State only the proven keys.** An earlier draft asserted the customer's key
  was `local_context_data`; the reproduction showed `vrf` for one direction and
  did not confirm the other. The claims now match the evidence.
- **This changes 2.8.6's scope.** The release plan said the crash was not
  fixed; it now is, and the plan is amended rather than left stating something
  false.

## Open

- The corrupted diffs are confined to per-ingestion branches, which are
  discarded on retry and never merged (merge replays ObjectChanges, not
  ChangeDiffs), so no persistent damage is expected in main. Not verified
  against the deployment's database.
