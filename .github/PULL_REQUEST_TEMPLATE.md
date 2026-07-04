<!--
Title must follow Conventional Commits (feat:, fix:, refactor:, chore:, docs:, test:).
CI enforces this plus the sensitive-content, harness, lint, and full-suite gates.
-->

## What & why

<!-- What does this change do, and why? -->

## Changes

-

## Validation

- [ ] Full plugin test suite passes (`invoke test`)
- [ ] Lint / harness / sensitive-content gates pass (`invoke lint`, `invoke harness-check`, `invoke sensitive-check`)
- [ ] For high-risk paths (utilities/, models, migrations): a plan doc under `docs/03_Plans/active/` is included
- [ ] No customer names, network IDs, snapshot IDs, or credentials in the diff
- [ ] `CHANGELOG.md` regenerated if the README compatibility table changed

## Compatibility / migration notes

<!-- Any schema migration, config change, or upgrade consideration for operators? -->
