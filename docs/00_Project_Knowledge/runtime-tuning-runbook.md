# Runtime Tuning Runbook (Generic)

Use this runbook for long-running or slow syncs in any deployment shape (Kubernetes, VM/systemd, or Docker).

This runbook is intentionally platform-agnostic. It defines what to tune, when to tune, and how to validate safely.

## Scope

Use this runbook when any of the following are true:

- throughput is below target (for example, `< 5 shards/hour` sustained)
- initial sync projection is operationally too long
- sync runtime crosses expected long-run thresholds (for example, 16h+) and you need confidence on stability and completion

## Guardrails

- Keep shard max-size protections enabled.
- Change one tuning batch at a time, then hold for at least 60 minutes.
- Keep `query_fetch_concurrency` between `1` and `16`.
- Keep `nqe_page_size` between `1` and `10000`.
- Roll back only the most recent increment if stability regresses.

## Data To Capture Every Hour

Capture and share these five values per hour:

1. shard index (`x/total`)
2. shards/hour over the last hour
3. ingestion issue count (new issues in that hour)
4. queue backlog depth
5. active worker count

Also capture correlation identifiers:

- sync ID
- execution run ID

## Tuning Trigger

If throughput remains below target after two hourly checkpoints (example: still `< 5 shards/hour`), apply one tuning batch.

## One Tuning Batch

Increase:

- worker count: `+50%` (round up)
- `query_fetch_concurrency`: `+25%` (round up), cap at `16`
- `nqe_page_size`: `+20%` (round up), cap at `10000`

Examples:

- workers `12 -> 18`
- `query_fetch_concurrency` `12 -> 15`
- `nqe_page_size` `8000 -> 9600`

If already at caps (`query_fetch_concurrency=16`, `nqe_page_size=10000`), tune workers first and re-measure before any other major runtime changes.

## Restart Scope

After applying one tuning batch:

- restart worker processes only
- do not perform a full control-plane restart unless required by your platform

The exact restart command depends on deployment type:

- Kubernetes: rollout restart worker deployment/statefulset only
- VM/systemd: restart only worker services
- Docker Compose: recreate/scale worker service only

## Post-Change Validation (60-Minute Hold)

For the next 60 minutes, verify:

1. shards/hour improved
2. ingestion issue rate remains low (example: `<= 2` new issues/hour)
3. queue backlog trend is stable or declining
4. shard max-size guard is still enforced

If issue rate spikes or guards fail, revert only the latest increment.

## Delete/Prune Phase Expectations

Delete-heavy work may appear later than early shard staging/apply. Do not assume delete failure early in the run.

When delete/prune phase begins, capture:

- deleted count
- `failed_change_count`
- first shard index where deletes are observed

If prune/delete phase starts and delete count remains zero unexpectedly, escalate with sync/run IDs and latest hourly metrics.

## Long-Run Stability Check

Treat long-run stability as separate from shard sizing behavior.

Confirm:

- run continues past prior barrier (for example, 16h) without timeout/restart loops
- no repeated terminal job timeout pattern
- recovery monitor signals are clean or actionable with bounded retries

## Operator Message Template

Use this copy/paste template:

```
Please keep the current run active and send hourly updates with:
- shard index x/total
- shards/hour (last hour)
- new ingestion issues (last hour)
- queue backlog depth
- active worker count
- sync ID and execution run ID

If throughput stays below 5 shards/hour for two hourly checkpoints:
- increase workers by 50% (round up)
- increase query_fetch_concurrency by 25% (cap 16)
- increase nqe_page_size by 20% (cap 10000)
- restart workers only

Then hold 60 minutes and report:
- updated shards/hour
- issue rate/hour
- backlog trend
- shard max-size guard status
```
