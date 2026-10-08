# Replace the Playwright UI harness with agent-browser

## Goal

Retire `playwright` as the UI-gate driver and run the same UI checks through
`agent-browser` (Vercel Labs, npm `agent-browser`). The release gate keeps one
canonical UI command; only its engine changes.

## Constraints

- Staged for the build after `3.0.4`. Do not land it inside the `3.0.4` release:
  that plan promises a version-bump-only commit, and the release authorization
  binds the UI evidence entry to the `playwright-test` command.
- `agent-browser` is a command-line browser driver, not a test runner. It has
  no `expect`; every assertion in `scripts/playwright_forward_ui.mjs` (about 120
  `expectVisible`/`assert` sites, the overflow checks, screenshots) must be
  re-expressed over its commands, with a non-zero exit on the first failure.
- `agent-browser` declares `node >=24`. Local Node is 26; confirm the release
  workflow's Node version before relying on it.
- The harness must stay isolated: `FORWARD_UI_HARNESS_ISOLATED=true`, the
  `forward-netbox-ui-test` compose project, and the seed command
  `forward_seed_ui_harness` are unchanged.
- No customer identifiers in fixtures, screenshots or artifacts.

## Touched Surfaces

- `scripts/playwright_forward_ui.mjs` (replaced), `package.json`,
  `package-lock.json`.
- `tasks.py`: `_run_playwright_ui`, `_run_playwright_in_isolated_runtime`,
  `playwright_test`, the `ci` task list, the `PLAYWRIGHT_*` environment names.
- `forward_netbox/management/commands/forward_seed_ui_harness.py`.
- `scripts/check_release_authorization.py` (the canonical UI gate and the
  host-port variable), `scripts/check_release_preflight.py` (the dependency
  check), `scripts/check_harness.py` (three `invoke playwright-test` references),
  `.github/workflows/release.yml` (`npx playwright install` at line 86).
- `scripts/tests/test_tasks.py`, `test_release_preflight.py`,
  `test_release_authorization.py`.
- `.gitignore`, `.dockerignore` (`.playwright-artifacts`).
- Docs: `release-playbook.md`, `validation-matrix.md`, `local-docker-workflow.md`,
  `agent-workflow.md`, `code-boundary-map.md`, `quality-score.md`,
  `harness-engineering-alignment.md`.

## Approach

1. Decide the task name. Recommended: `invoke ui-test`, with `playwright-test`
   removed rather than aliased, so no gate can still name the old engine.
2. Port the checks in order: login, sync list, sync detail panels, the
   drift-policy and workload pages, support-bundle export, then the mobile and
   overflow checks. Prefer `agent-browser`'s text/accessibility snapshot for
   "is visible" and keep screenshots as artifacts.
3. Rename the environment variables once, in `tasks.py` and the authorization
   check together, and update the authorization binding so the UI entry still
   requires the canonical command.
4. Swap the dependency in `package.json`, regenerate `package-lock.json`, update
   the preflight dependency check and the release workflow's browser install.
5. Update tests and docs in the same change. Add a negative test: a deliberately
   missing text must fail the harness with a non-zero exit.

## Validation

- `invoke harness-check`, `invoke harness-test`, `invoke lint`.
- `scripts/tests` for tasks, preflight and authorization.
- The new UI task against the isolated runtime, once green and once with an
  injected failing assertion that must exit non-zero.
- `invoke ci` and `invoke artifact-test` on the final tree, since the gate
  definition changes.

## Rollback

Revert the commit; `package.json` returns to `playwright 1.59.1` and the old
script and task names come back together. No migration or runtime state.

## Decision Log

- **Separate change, not part of 3.0.4.** The in-flight release gate and its
  authorization evidence name `playwright-test`.
- **Rename, do not alias.** An alias would let docs and authorization keep
  naming an engine that no longer runs.
- **Open:** whether `agent-browser` can run headless in the release workflow
  runner without a system Chrome install step.
