# FHRP Group Shard Co-location (churn fix)

## Goal

Stop `ipam.fhrpgroup` from churning (Created 13 / Deleted 13 the same HSRP groups
every sync). The 1.5.8 `_split_diff_rows` dedup was correct but never fired for
FHRP because the two halves of the churn were in different shards.

## Constraints

- A genuinely removed group must still be deleted.
- No change to the FHRP NQE or the apply/delete code.
- No regression to other device-sharded models.

## Findings

FHRP is device-sharded (`branch_budget.DEVICE_SHARD_MODELS`). An HSRP group's
active and standby routers are different devices, so they land in different
shards. Each shard re-fetches its own device-scoped diff
(`multi_branch_executor._load_execution_context(shard_scope=...)`), so the group's
ADD (active router, post state-flap) and DELETE (standby router) halves are never
in the same fetch batch. `_split_diff_rows`'s same-batch dedup can't pair them, so
the spurious DELETE survives and `delete_ipam_fhrpgroup` drops the group another
shard just recreated.

## Touched Surfaces

- `forward_netbox/utilities/branch_budget.py`:
  - `row_shard_key` — FHRP buckets by group identity
    (`fhrp:<protocol>|<group_id>|<address>|<vrf>`), not device, so both routers
    share one shard.
  - `shard_fetch_contract` — FHRP routes to `model` fetch (full diff) rather than
    per-device `nqe_parameters`, so the un-sharded diff is co-resident.
  - `_build_shard_fetch_model_contracts` — declares FHRP's `model`/`fhrp_identity`
    contract so the static contract matches actual behavior.
- `forward_netbox/tests/test_sync_runner_contracts.py` — co-location + model-fetch
  tests.

## Approach

Co-locate both routers of a group in one shard by bucketing on group identity, and
fetch FHRP as a full model diff. The existing 1.5.8 `_split_diff_rows` dedup then
sees the ADD and DELETE together and drops the stale DELETE. FHRP keeps its
`DEVICE_SHARD_MODELS` membership (for the apply-engine/local-safety contracts);
only the key shape and fetch mode are overridden. FHRP is low-volume, so losing
per-device NQE pushdown is negligible.

## Validation

- `forward_netbox.tests.test_sync_runner_contracts` (co-location, model fetch, plus
  the existing dedup + genuine-delete tests).
- Full `forward_netbox.tests` suite (1176, green).
- End-to-end confirmation on the next CustomerOrg sync (expect 0 FHRP create/delete on a
  no-change re-sync).

## Rollback

Revert the three `branch_budget.py` edits and the tests; FHRP returns to
device-sharding (and the churn).

## Decision Log

- Co-locate via identity bucket (reuses the shipped 1.5.8 dedup) over a delete-time
  live snapshot check: the latter has no cheap full-result source at per-shard
  apply time and pushes churn logic into the apply layer.
- Keep FHRP in `DEVICE_SHARD_MODELS` and override the three call sites: least
  disruptive to the other contract consumers.
