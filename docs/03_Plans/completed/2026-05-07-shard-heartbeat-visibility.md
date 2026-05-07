# Shard Heartbeat Visibility for 0.7.0

## Goal

Make long-running branch shards visibly alive in the UI and job logs so operators can tell the difference between a slow shard and a stalled run, without changing row application or branch semantics.

## Constraints

- Preserve the current row-level continue-on-error contract.
- Do not change branch execution, retry, or merge behavior.
- Keep the change NetBox-native and Branching-native.
- Do not add customer-specific handling or identifiers.

## Touched Surfaces

- `forward_netbox/utilities/sync_execution.py`
- `forward_netbox/utilities/sync_reporting.py`
- `forward_netbox/utilities/multi_branch_lifecycle.py`
- `forward_netbox/utilities/sync_state.py`
- `forward_netbox/utilities/ingestion_presentation.py`
- `forward_netbox/utilities/execution_telemetry.py`
- `forward_netbox/tests/test_sync_state.py`
- `forward_netbox/tests/test_sync.py`
- `ARCHITECTURE.md`
- `docs/03_Plans/technical-debt.md`

## Approach

1. Add a lightweight shard heartbeat helper that records a `last_progress_at` timestamp and the current model/shard context in branch-run state.
2. Emit heartbeat updates from the row-processing loop at a modest interval so the job log and cached UI state move forward during long batches.
3. Extend the branch-run summary and activity text so the UI can show "alive but busy" instead of only the last phase message.
4. Add focused regressions for the heartbeat state and the activity text.
5. Update architecture/debt notes if the visibility surface becomes an explicit boundary.

## Validation

- `python -m compileall forward_netbox/utilities/sync_state.py forward_netbox/utilities/sync_reporting.py forward_netbox/utilities/execution_telemetry.py forward_netbox/utilities/multi_branch_lifecycle.py forward_netbox/tests/test_sync_state.py forward_netbox/tests/test_sync.py`
- `invoke test`
- `invoke lint`
- `invoke docs`
- `invoke ci`

## Rollback

- Remove the heartbeat helper and the added state keys.
- Restore the previous activity text and progress card behavior.
- Revert the tests and docs if the visibility pass changes UI semantics.

## Decision Log

- The screenshot shows the run is likely alive but quiet, so the next useful improvement is visibility rather than a behavior fix.
- A single shard heartbeat is preferable to a broader telemetry rewrite because the current statistics already exist and only need freshness cues.
