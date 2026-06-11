# Release Playbook

Use this playbook for direct release pushes.

## Preconditions

- Worktree is clean except for intended release changes.
- Version is updated in `pyproject.toml` and `forward_netbox/__init__.py`.
- Release notes are updated in `README.md`, `docs/README.md`, and `docs/01_User_Guide/README.md`.
- No customer identifiers, network IDs, snapshot IDs, credentials, or private screenshots are in tracked content.

## Local Gate

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
invoke release-dataset-gate --dataset-label=release-smoke
python -m build
```

For full parity with CI:

```bash
invoke ci
```

GitHub-hosted CI intentionally skips the Playwright browser install and UI
harness because hosted runner browser setup has been slower than the product
checks it protects. Keep browser validation in the local release gate with
`invoke playwright-test` or `invoke ci`, and record that evidence before
publishing.

For Branching recovery or orchestration changes, also run the opt-in destructive
worker-kill harness and capture support-bundle evidence:

```bash
export FORWARD_CHAOS_SYNC_NAME=<active-chaos-sync-name>
export FORWARD_CHAOS_OUTPUT_DIR=docs/03_Plans/evidence/chaos
invoke docker-chaos-kill --scenario=stage-before-branch --confirm
invoke docker-chaos-kill --scenario=stage-after-branch --confirm
invoke docker-chaos-kill --scenario=stage-during-apply --confirm
invoke docker-chaos-kill --scenario=merge-during-exec --confirm
```

Each scenario must leave both a `chaos-<scenario>-run-*.json` support bundle and
a `chaos-<scenario>-metadata-*.json` kill metadata file containing the killed
worker/container ID, execution run ID, active step ID, branch ID/name when
present, recovery action, and `support_bundle_recovery_verified: true`.

For query-pushdown or shard-scope performance changes, capture at least one live
pushdown profile report and attach it to release notes:

```bash
invoke pushdown-profile --sync-name "ui-harness-sync" --model "dcim.interface" --output-json /tmp/pushdown-dcim-interface.json
invoke pushdown-profile --sync-name "ui-harness-sync" --top-slow-models 5 --output-json /tmp/pushdown-top-slow-models.json
invoke scale-benchmark --sync-name "ui-harness-sync" --output-json docs/03_Plans/evidence/scale-benchmark.json
```

For operational scale runs, keep source-level query concurrency conservative by
default (`query_fetch_concurrency=6`) and increase gradually only when DB and
worker telemetry confirms headroom. Use the Health tab runtime checks to detect
high-concurrency contention risk.

For repeated soak execution rehearsal, run:

```bash
invoke scale-soak --runs 3 --execution-backend branching --max-changes-per-branch 10000
```

Before any `1.1.x` release, refresh field-scale evidence against the release-validation
dataset and enforce the gate:

```bash
export FORWARD_SMOKE_DATASET_LABEL=release-smoke
invoke release-runtime-preflight --dataset-label=release-smoke
invoke field-scale-runtime-matrix --resume=False
invoke release-dataset-gate --dataset-label=release-smoke
invoke release-readiness-audit --dataset-label=release-smoke
```

`release-dataset-gate` fails when the field-scale artifact is stale, not
`passed`, not labeled as the release-validation dataset, missing required matrix steps, or
produced with `resume=True`.
When validation credentials are available, `release-readiness-audit` also runs
the validation-org query audit so the shipped query set is compared against the
live validation folder before publish.
When local runtime dependencies are unavailable, matrix evidence now records
`preflight_failure_code` (for example `docker_api_unreachable`) before running
the three smoke steps.

## Publish Flow

1. Commit with a lore-style message that includes `Tested:` and `Not-tested:` trailers.
2. Push `main`.
3. Create and push an annotated tag, for example `v0.3.0.1`.
4. Wait for GitHub CI on both `main` and the tag. Treat GitHub CI as the
   non-browser hosted gate; the Playwright UI harness is proven locally.
5. Build artifacts from the tagged commit.
6. Create the GitHub Release using a notes file or carefully quoted notes.
7. Upload the same artifacts to PyPI with `twine`.
8. Verify the GitHub Release assets and PyPI project page.

Avoid shell backticks in inline `gh release create --notes` text. Prefer a notes file to prevent accidental shell expansion.
