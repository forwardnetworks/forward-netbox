# Release Playbook

Use this playbook for reviewed production releases.

## Preconditions

- Worktree is clean except for intended release changes.
- Version is updated in `pyproject.toml` and `forward_netbox/__init__.py`.
- Release notes are updated in `README.md`, `docs/README.md`, and `docs/01_User_Guide/README.md`.
- No customer identifiers, network IDs, snapshot IDs, credentials, or private screenshots are in tracked content.
- Repository rulesets `main-release-integrity`, `version-tag-integrity`, and the
  version-tag creation restriction are active. Main has no bypass actors and
  requires a current CODEOWNERS approval, resolved conversations, the trusted
  candidate scan, exact NetBox 4.6.5 CI, both CodeQL analyses, and the separate
  GitHub Advanced Security CodeQL result. Version tags
  reject deletion and movement. Human credentials cannot create them; only the
  environment-scoped deploy key used by the frozen protected-main tag workflow
  can create a matching tag.
- Annotated tag `security-bootstrap-2.6` resolves to the reviewed bootstrap
  commit and is protected against deletion or movement. The release workflow,
  trusted PR scanner, protected-main tag workflow and authorizer, provenance
  verifier, reproducible builder, and hashed release-tool lock must be
  byte-identical to that anchor.
- Environment `release-tag` is restricted to protected `main`, requires the
  independent reviewer with self-review and administrator bypass disabled, and
  is the only location of `RELEASE_TAG_DEPLOY_KEY`. Environment `pypi` accepts
  only `v*` tags and has the same independent approval and no-bypass controls.
- `.github/CODEOWNERS` names a valid accountable owner, the repository-level
  `FORWARD_SENSITIVE_PATTERNS` Actions secret contains at least one private
  pattern, and the `FORWARD_SENSITIVE_HISTORY_BASELINE` repository variable
  exactly matches `.sensitive-history-baseline`.
- Every checked release-authorization entry records its evidence-class-specific
  command, a retrospective success outcome, and a numeric result from the final
  tree. The authorizer binds CI, artifact, scale/failure, UI, ownership,
  customer-acceptance, and independent-review entries to their canonical gates;
  an unrelated successful command cannot authorize them. Prospective checklist
  language cannot authorize a release.
- The trusted tag authorizer's live-control preflight passes. It reads the
  repository settings, required authenticated statuses, all four tag rulesets,
  both deployment environments and policies, and Actions SHA-pinning setting
  from GitHub before it authorizes a tag.

For the one-time 2.6 trust bootstrap, merge the bootstrap PR and create
`security-bootstrap-2.6` while the four public CI/CodeQL statuses are required.
Immediately afterward, add authenticated `Trusted sensitive-content scan` to
`main-release-integrity` before opening the production PR. Version-tag
authorization fails closed until that fifth status is required.

## Local Gate

```bash
invoke harness-check
invoke harness-test
invoke lint
invoke check
invoke scenario-test
invoke test
invoke playwright-test
invoke docs
invoke package
python -m twine check dist/*
invoke artifact-test
invoke ci
```

Run the exact-version migration and installation checks in a fresh NetBox
`4.6.5` / Branching `1.1.1` runtime. For merge, ownership, recovery, or
orchestration changes, the scenario and full suites must include crash/retry,
partial-merge, post-merge-resume, stale-generation, and stuck-job recovery
coverage.

`invoke package` performs two isolated builds with the commit timestamp as
`SOURCE_DATE_EPOCH` and fails unless both wheel and sdist SHA-256 digests are
byte-identical. CI and release jobs install Python controller tools only from
`requirements-release.txt` with `--require-hashes`.

For a configured validation source, verify shipped query publication and run a
customer-equivalent sync through terminal state:

```bash
invoke validation-org-query-audit --source-name '<validation source>' --fail-on-gap
invoke smoke-sync --plan-only
invoke smoke-sync
invoke sync-release-gate --sync-ids '<sync id>'
```

For query-pushdown or partition-scope performance changes, capture a live
pushdown profile:

```bash
invoke pushdown-profile --sync-name "ui-harness-sync" --model "dcim.interface" --output-json /tmp/pushdown-dcim-interface.json
invoke pushdown-profile --sync-name "ui-harness-sync" --top-slow-models 5 --output-json /tmp/pushdown-top-slow-models.json
```

For operational scale runs, keep source-level query concurrency conservative by
default (`query_fetch_concurrency=6`) and increase gradually only when DB and
worker telemetry confirms headroom. Use the Health tab runtime checks to detect
high-concurrency contention risk.

For repeated soak execution rehearsal, run:

```bash
invoke scale-soak --runs 3 --max-changes-per-staging-item 10000
```

## Publish Flow

1. Run `invoke release --version X.Y.Z --summary "..." --write` and complete the
   local gate above.
2. Run the same command with `--publish`; it creates or updates the release
   branch, then waits for successful GitHub CI runs from the exact `ci.yml` and
   `codeql.yml` workflow identities on the exact branch commit.
3. Run it with `--finish`; the first finish promotes candidate metadata, pushes
   it, waits for CI on that exact commit, and stops without updating `main` or
   creating a tag.
4. Run `--finish` again. It opens the production PR, requests the independent
   CODEOWNER, and enables squash auto-merge. GitHub will not merge it until the
   approval, trusted base-branch scanner status, CI, and CodeQL requirements all
   pass.
5. After that PR is on `main`, run every final-tree gate on that exact main
   commit. Create `release/X.Y.Z-evidence` from it, record its full SHA as
   `Evidence base commit`, and make one commit that changes only the release
   plan. Run `--finish` on that branch to open the separately reviewed evidence
   PR. Its squash merge preserves the evidence-only parent relationship.
6. Update local `main` to that reviewed evidence commit and run `--finish` once
   more. It checks the authorization binding and exact main-push workflows,
   then dispatches `.github/workflows/trusted-tag.yml` from the exact protected
   `main` commit. After independent environment approval, that frozen workflow
   re-proves the bootstrap or release lineage and uses the environment-only
   deploy key to create the immutable annotated tag. Human credentials cannot
   push it. A concurrent protected-main advance does not invalidate an already
   authorized ancestor tag; divergence still fails closed. The tag workflow
   independently proves that both the production and
   evidence commits came from approved main PRs. It executes the provenance
   verifier from `security-bootstrap-2.6`, walks every
   first-parent main commit after that anchor, pages every review and status,
   binds each trusted status to the exact successful `pull_request_target` run,
   PR number, and candidate SHA, and requires exact CI/CodeQL workflows on every
   reviewed main commit.
7. The tag workflow installs only the hashed release-tool lock, builds the
   wheel/sdist twice, rejects any byte mismatch, installed-runtime-tests the
   identical pair, generates and validates the full runtime SBOM, publishes the
   pair to PyPI, and creates the GitHub release from the same workflow artifacts.
8. The release command waits for the tag workflow to finish. Independently
   verify the tag, `main`, PyPI hashes, GitHub asset hashes, and attached SBOM.
