# Runtime Tuning Runbook (Generic)

Use this runbook for long-running or slow syncs in any deployment shape (Kubernetes, VM/systemd, or Docker).

This runbook is intentionally platform-agnostic. It defines what to tune, when to tune, and how to validate safely.

In this runbook, a workload shard is one bounded staging plan item. Every
workload shard for a sync targets the same single Branching branch.

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
- ingestion ID and NetBox job ID

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

## Sync Health Adaptive Capacity

The Sync Health `Large Run Tuning` section includes an adaptive capacity
decision:

- `recommend_tuning_batch`: throughput is below target, issue rate is safe,
  and worker/database headroom evidence is available. Apply exactly one tuning
  batch.
- `hold_current_settings`: throughput is at or above target. Keep the current
  settings and continue hourly checks.
- `rollback_latest_tuning_batch`: issue rate is above the safe threshold.
  Revert only the most recent tuning increment, restart workers only, then hold
  for 60 minutes.
- `insufficient_evidence`: throughput or issue-rate data is missing, or worker
  count/database headroom is not observable from the deployment evidence.
- `capacity_blocked`: the deployment evidence shows worker or database
  saturation. Fix that bottleneck before increasing concurrency.

When Health reports insufficient capacity evidence, collect the same hourly
fields from the operator message template, plus:

- active NetBox worker count
- queue backlog depth or trend
- database headroom for the same time window

## Restart Scope

After applying one tuning batch:

- restart worker processes only
- do not perform a full control-plane restart unless required by your platform

The exact restart command depends on deployment type:

- Kubernetes: rollout restart worker deployment/statefulset only
- VM/systemd: restart only worker services
- Docker Compose: recreate/scale worker service only

Examples:

```bash
# Kubernetes
kubectl scale deployment/netbox-worker --replicas <new-worker-count>
kubectl rollout restart deployment/netbox-worker

# VM/systemd
sudo systemctl restart netbox-rq-worker

# Docker Compose
docker compose up -d --scale netbox-worker=<new-worker-count> netbox-worker
```

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

If prune/delete phase starts and delete count remains zero unexpectedly, escalate with the sync, ingestion, and NetBox job IDs plus the latest hourly metrics.

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
- sync ID, ingestion ID, and NetBox job ID

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
