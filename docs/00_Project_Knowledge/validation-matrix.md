# Validation Matrix

Run the smallest gate that proves the change, then run the release gate before publishing.

| Change type | Required validation |
| --- | --- |
| Documentation only | `invoke harness-check`, `invoke harness-test`, `invoke docs` |
| Query map or NQE helper change | `invoke harness-check`, `invoke harness-test`, `invoke lint`, `invoke test`, update built-in NQE reference |
| Forward API client change | `invoke lint`, `invoke check`, `invoke test` |
| Sync planning or branch budget change | `invoke lint`, `invoke check`, `invoke scenario-test`, `invoke test`, local Docker sync smoke test |
| Branching ledger, recovery, or scale behavior change | `invoke lint`, `invoke check`, `invoke scenario-test`, `invoke scale-chaos-test`, `invoke test`, `invoke playwright-test` when UI/API surfaces change; `scale-chaos-test` includes focused recovery and support-bundle export coverage |
| Validation or drift policy change | `invoke lint`, `invoke check`, `invoke scenario-test`, `invoke test`, `invoke playwright-test`, validation-only smoke test, force-allow override smoke test when the change adds a break-glass path |
| NetBox model adapter change | `invoke lint`, `invoke check`, `invoke scenario-test`, `invoke test`, targeted local Docker sync |
| UI/API workflow change | `invoke lint`, `invoke check`, `invoke test`, `invoke playwright-test`, browser/UI verification when visible behavior changes |
| Release | `invoke ci`, GitHub CI success on `main` and tag |

## Core Commands

```bash
invoke harness-check
invoke harness-test
invoke lint
invoke check
invoke scenario-test
invoke scale-chaos-test
invoke test
invoke playwright-test
invoke docs
invoke package
invoke ci
```

## Destructive Chaos Harness (Opt-In)

Use the destructive worker-kill harness only for deferred-risk recovery proof.
It is intentionally excluded from `invoke ci`.

```bash
invoke docker-chaos-kill --scenario=stage-before-branch --confirm
invoke docker-chaos-kill --scenario=stage-after-branch --confirm
invoke docker-chaos-kill --scenario=stage-during-apply --confirm
invoke docker-chaos-kill --scenario=merge-during-exec --confirm
```

Optional scenario-aware controls:
- `FORWARD_CHAOS_SYNC_NAME=<sync-name>` waits for scenario readiness on the active execution run.
- `FORWARD_CHAOS_OUTPUT_DIR=/tmp/chaos` exports execution-run support bundles for post-kill evidence.
- `FORWARD_CHAOS_WAIT_SECONDS` / `FORWARD_CHAOS_POLL_SECONDS` tune readiness polling.

## Live Pushdown Proof (Opt-In)

Use this command to gather live runtime/parity evidence for shard pushdown on a
specific model using an existing sync configuration:

```bash
invoke pushdown-profile --sync-name "ui-harness-sync" --model "dcim.interface"
```

Optional flags:
- `--query-name "Forward Interfaces"` when multiple maps are bound to the model.
- `--sample-shard-keys 500` to widen shard sample size.
- `--output-json /tmp/pushdown-profile.json` to persist the report artifact.

### Architecture Audit Artifact

Use this to emit a single JSON artifact that captures the current apply-engine
model matrix (bulk-ORM safe set + adapter-required blockers) and optional
sync/runtime evidence:

```bash
invoke architecture-audit
invoke architecture-audit --sync-name "ui-harness-sync" --output-json /tmp/architecture-audit.json
invoke architecture-audit --fail-on-gap
invoke architecture-audit-check
invoke architecture-runtime-evidence
invoke architecture-completion-audit --output-json /tmp/architecture-completion-audit.json
```
- `invoke architecture-runtime-evidence` writes
  `docs/03_Plans/evidence/architecture-runtime-evidence.json`. Completion audit
  consumes it via `--runtime-evidence` (default path) and marks runtime checks
  complete when the evidence is fresh and passed.
- Use `invoke architecture-runtime-evidence --run-adp` to execute the ADP
  scale runtime matrix when `FORWARD_SMOKE_USERNAME`,
  `FORWARD_SMOKE_PASSWORD`, and `FORWARD_SMOKE_NETWORK_ID` are set in the
  environment.
- `--top-slow-models 5` to automatically profile the slowest recent models from execution-step history.

Operational default:
- keep source `query_fetch_concurrency` at `6` initially and raise only with
  observed DB/worker headroom.

For repeated operational soak runs (manual, opt-in):

```bash
invoke scale-soak --runs 3 --execution-backend branching --max-changes-per-branch 10000
```

## Sensitive-Content Gate

The sensitive-content guard must stay in local and CI validation:

```bash
python scripts/check_sensitive_content.py
python scripts/check_sensitive_content.py --all-history
```

Use `.sensitive-patterns.local.txt` for local-only customer names, tenant labels, network IDs, or other identifiers that should never be committed.
