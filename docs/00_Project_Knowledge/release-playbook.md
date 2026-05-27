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
python -m build
```

For full parity with CI:

```bash
invoke ci
```

For Branching recovery or orchestration changes, also run the opt-in destructive
worker-kill harness and capture support-bundle evidence:

```bash
invoke docker-chaos-kill --scenario=stage-before-branch --confirm
invoke docker-chaos-kill --scenario=stage-after-branch --confirm
invoke docker-chaos-kill --scenario=stage-during-apply --confirm
invoke docker-chaos-kill --scenario=merge-during-exec --confirm
```

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

## Publish Flow

1. Commit with a lore-style message that includes `Tested:` and `Not-tested:` trailers.
2. Push `main`.
3. Create and push an annotated tag, for example `v0.3.0.1`.
4. Wait for GitHub CI on both `main` and the tag.
5. Build artifacts from the tagged commit.
6. Create the GitHub Release using a notes file or carefully quoted notes.
7. Upload the same artifacts to PyPI with `twine`.
8. Verify the GitHub Release assets and PyPI project page.

Avoid shell backticks in inline `gh release create --notes` text. Prefer a notes file to prevent accidental shell expansion.
