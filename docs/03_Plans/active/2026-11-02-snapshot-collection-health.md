# Snapshot collection health in blocked validation runs

## Goal

When validation refuses a run because row counts fell, say whether the cause is the snapshot: devices in this sync's scope that Forward did not collect (authentication failed, connection timed out, parser exception), compared with the last baseline snapshot. A customer's sync was blocked for two days by five row-count drops; the real cause was about 600 in-scope devices failing authentication in one Forward snapshot, and the next snapshot had recovered. Nothing in the bundle or the run said so.

## Constraints

- No customer names, network ids, snapshot ids, hostnames, tag names or addresses in the repo, tests or docs. Synthetic fixtures only.
- The bundle stays name-free: counts and failure slugs only.
- Read-only Forward calls through the SDK. At most two extra NQE calls, and only on a run that is already blocked on a row-count drop. A passing run makes no extra call.
- Best effort: a failure of the probe never fails or unblocks validation, and is recorded as an error string, never read as zero.
- The existing whole-snapshot collection-collapse guard (50%) is unchanged.

## Touched Surfaces

- `bundle_diagnostics.py`: `snapshot_collection_trend` (in-scope completed and not-collected devices, run over run, with failure-reason deltas, from the stored scope reports, no Forward call) and `include_tag_timeline` (devices that gained or lost the sync's include tags by hour, split by sync run or other, counts only).
- `scope_reconciliation.py`: the failure-reason token no longer keeps a trailing bracket and reads processing failures.

- `forward_netbox/utilities/snapshot_collection_health.py` (new): per-scope collection counts, the comparison, the reason sentence.
- `forward_netbox/utilities/validation.py`: leads a blocked run's reasons with the snapshot finding; stores the counts on the run.
- `forward_netbox/utilities/validation_guidance.py`: the next step for the new reason.
- `health_summary_blocks.py`: `health.latest_validation.snapshot_collection` carries the counts. No new UI figure, so no label-index entry.
- Tests: `test_snapshot_collection_health.py`, `test_validation_reason_export.py`.
- Docs: `docs/01_User_Guide/troubleshooting.md`.

## Approach

1. Count the sync's in-scope devices by `snapshotInfo.result` for the target snapshot and the last baseline ingestion's snapshot, using the sync's own include and exclude tags.
2. If completed in-scope devices fell and the not-completed count rose by at least 5, add "Snapshot collection problem" as the first blocking reason, with both counts and the top failure types.
3. Guidance: wait for the next snapshot or fix collection in Forward, then run again; Force-Allow only if the devices are really gone.
4. Store both breakdowns on the validation run so the bundle can carry them.

## Validation

- Unit tests derived from the requirement: finding when scope collection fell, none when it did not, none when baseline is unknown, probe error is not a zero.
- Validation-runner tests: reason leads a shrink block, no probe on a passing run, probe error leaves the shrink reasons as they were.
- `invoke harness-check`, `pre-commit`, the Django suite once at the end.
- Not verified: the probe against a live Forward until a customer sync hits it.

## Rollback

Revert the change. The reason is additive; older runs keep their reasons.

## Decision Log

- Per-model change counts after a merge are already exported in `health.recent_ingestions` (from the change log by request id). What was missing is the staged side: `merge.py` now records the merge's logical changes by model and the deletes a protecting reference held back (aggregated per model) on the job the logger owns, and `delete_accounting` reconciles them with the logged deletes per model and names the legitimate causes of a difference.
- Include-tag changes are attributed by the sync's ingestion request ids, not by user, because a manual merge replays under the operator's user.

- forward-sdk moves 0.1.29 to 0.1.31 (0.1.30 adds `nqe.execution`, `execute_and_wait` and `forward_sdk.testing`; 0.1.31 adds unpublished licensing, Tenable and FDB routes; nothing the plugin calls changed). `development/constraints-upgrade-from.txt` moves to 0.1.29, the pin 3.0.11 shipped.

- The finding keys on the rise in devices that did not complete, not the fall in completed: a retired site must not be told it is a collection problem.
- The probe has a 60 s deadline so a slow Forward cannot turn a blocked run into a failed one.
- SNMP endpoints are not counted; endpoint-owned shrinkage stays unexplained.
- Probe only on a block, to respect the Forward call budget.
- Reason text, not a new status: the block already exists and the sentence is what the operator reads.
